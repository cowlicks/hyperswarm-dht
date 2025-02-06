//! Rust Implementation of the hyperswarm DHT
#![warn(rust_2018_idioms)]
#![allow(unreachable_code)]

use core::cmp;
use std::{
    array::TryFromSliceError,
    convert::{TryFrom, TryInto},
    fmt,
    net::{AddrParseError, IpAddr, SocketAddr, SocketAddrV4, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use compact_encoding::{types::CompactEncodable, EncodingError};
use crypto::{sign_announce_or_unannounce, Keypair2};
use dht_rpc::{commit::CommitMessage, query::Query};
use ed25519_dalek::{Keypair, PublicKey};
use fnv::FnvHashMap;
use futures::{
    channel::mpsc::{self},
    task::{Context, Poll},
    Stream,
};
use lookup::LookupResponse;
use prost::Message as ProstMessage;
pub use queries::QueryOpts;
use smallvec::alloc::collections::VecDeque;
use tokio::sync::oneshot::error::RecvError;
use tracing::{error, instrument, trace, warn};

use crate::{
    dht_proto::{PeersInput, PeersOutput},
    lru::{CacheKey, PeerCache},
    store::Store,
};
pub use ::dht_rpc::{
    cenc::generic_hash,
    commit::Commit,
    peers::{decode_local_peers, decode_peers, PeersEncoding},
    query::{CommandQuery, QueryId, QueryStats},
    Command, DhtConfig, ExternalCommand, IdBytes, Peer, PeerId, RequestMsgData, RequestOk,
    Response, ResponseOk, RpcDht, RpcDhtBuilderError, RpcDhtEvent,
};

mod dht_proto {
    use prost::Message;

    include!(concat!(env!("OUT_DIR"), "/dht_pb.rs"));
}
pub mod cenc;
pub mod crypto;
mod lookup;
pub mod lru;
mod queries;
pub mod store;

#[allow(dead_code)]
const EPH_AFTER: u64 = 1000 * 60 * 20;

/// The publicly available hyperswarm DHT addresses
pub const DEFAULT_BOOTSTRAP: [&str; 3] = [
    "node1.hyperdht.org:49737",
    "node2.hyperdht.org:49737",
    "node3.hyperdht.org:49737",
];

pub(crate) const ERR_INVALID_INPUT: usize = 7;
pub(crate) const ERR_INVALID_SEQ: usize = 11;
pub(crate) const ERR_SEQ_MUST_EXCEED_CURRENT: usize = 13;

pub mod commands {
    pub const PEER_HANDSHAKE: usize = 0;
    pub const PEER_HOLEPUNCH: usize = 1;
    pub const FIND_PEER: usize = 2;
    pub const LOOKUP: usize = 3;
    pub const ANNOUNCE: usize = 4;
    pub const UNANNOUNCE: usize = 5;
}
/// The command identifier for `Mutable` storage
pub const MUTABLE_STORE_CMD: usize = 1;
/// The command identifier for immutable storage
pub const IMMUTABLE_STORE_CMD: usize = 2;
/// The command identifier to (un)announce/lookup peers
pub const PEERS_CMD: usize = 3;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error from dht_rpc: {0}")]
    RpcError(#[from] ::dht_rpc::Error),
    #[error("Error from compact_encoding: {0}")]
    CompactEncodingError(EncodingError),
    #[error("IO Eror")]
    IoError(#[from] std::io::Error),
    #[error("Invalid RPC command in message: {0}")]
    InvalidRpcCommand(u8),
    #[error("Incorrect message ID size. Expected 32. Error: {0}")]
    IncorrectMessageIdSize(TryFromSliceError),
    #[error("Error in libsodium's genric_hash function. Return value: {0}")]
    LibSodiumGenericHashError(i32),
    #[error("RpcDhtBuilderError: {0}")]
    RpcDhtBuilderError(#[from] RpcDhtBuilderError),
    #[error("RecvError: {0}")]
    RecvError(#[from] RecvError),
    #[error("AddrParseError: {0}")]
    AddrParseError(#[from] AddrParseError),
    #[error("Requests must have a 'to' field")]
    RequestRequiresToField,
    #[error("Ipv6 not supported")]
    Ipv6NotSupported,
}

pub type Result<T> = std::result::Result<T, Error>;

/// TODO make EncodingError impl Error trait
impl From<EncodingError> for Error {
    fn from(value: EncodingError) -> Self {
        Error::CompactEncodingError(value)
    }
}

/// The implementation of the hyperswarm DHT
#[derive(Debug)]
pub struct HyperDht {
    /// The underlying Rpc DHT including IO
    inner: RpcDht,
    /// Map to track the queries currently in progress
    queries: FnvHashMap<QueryId, QueryStreamType>,
    /// If `true`, the node will become non-ephemeral after the node has shown
    /// to be long-lived
    #[allow(unused)] // FIXME why aint this used
    adaptive: bool,
    /// Cache for known peers
    peers: PeerCache,
    /// Storage for the mutable/immutable values
    store: Store,
    /// Queued events to return when being polled.
    queued_events: VecDeque<HyperDhtEvent>,
}

impl HyperDht {
    /// Create a new DHT based on the configuration
    pub async fn with_config(mut config: DhtConfig) -> Result<Self> {
        config = config.register_commands(&[MUTABLE_STORE_CMD, IMMUTABLE_STORE_CMD, PEERS_CMD]);

        if config.bootstrap_nodes.is_empty() {
            for addr_str in DEFAULT_BOOTSTRAP.iter() {
                if let Some(addr) = addr_str.to_socket_addrs()?.last() {
                    config.bootstrap_nodes.push(addr)
                }
            }
        }

        Ok(Self {
            adaptive: config.adaptive,
            queries: Default::default(),
            inner: RpcDht::with_config(config).await?,
            // peer cache with 25 min timeout
            peers: PeerCache::new(65536, Duration::from_secs(60 * 25)),
            store: Store::new(5000),
            queued_events: Default::default(),
        })
    }

    /// The local address of the underlying `UdpSocket`
    #[inline]
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.inner.local_addr()?)
    }

    /// Handle an incoming requests for the registered commands and reply.
    fn on_command(&mut self, q: CommandQuery, request: RequestMsgData, peer: Peer) {
        match q.command {
            MUTABLE_STORE_CMD => {
                let resp = self.store.on_command_mut(q);
                self.inner.reply_command(resp)
            }
            IMMUTABLE_STORE_CMD => {
                let resp = self.store.on_command(q);
                self.inner.reply_command(resp)
            }
            PEERS_CMD => self.on_peers(q),
            command => self
                .queued_events
                .push_back(HyperDhtEvent::CustomCommandQuery {
                    command,
                    msg: Box::new(request),
                    peer,
                }),
        }
    }

    pub fn find_peer(&mut self, pub_key: PublicKey) -> QueryId {
        let target = IdBytes(generic_hash(pub_key.as_bytes()));

        self.inner.query(
            Command::External(ExternalCommand(commands::FIND_PEER)),
            target,
            None,
            Commit::No,
        )
    }

    /// Callback for an incoming `peers` command query
    fn on_peers(&mut self, mut query: CommandQuery) {
        // decode the received value
        if let Some(ref val) = query.value {
            if let Ok(peer) = PeersInput::decode(&**val) {
                // callback
                let port = peer
                    .port
                    .and_then(|port| u16::try_from(port).ok())
                    .unwrap_or_else(|| query.peer.addr.port());

                if let IpAddr::V4(host) = query.peer.addr.ip() {
                    let from = SocketAddr::V4(SocketAddrV4::new(host, port));

                    let remote_cache = CacheKey::Remote(query.target.clone());

                    let local_cache = peer.local_address.as_ref().and_then(|l| {
                        if l.len() == 6 {
                            let prefix: [u8; 2] = l[0..2].try_into().unwrap();
                            let suffix: [u8; 4] = l[2..].try_into().unwrap();
                            Some((
                                CacheKey::Local {
                                    id: query.target.clone(),
                                    prefix,
                                },
                                suffix,
                            ))
                        } else {
                            None
                        }
                    });

                    let local_peers = if let Some((local_cache, suffix)) = local_cache {
                        self.peers.get(&local_cache).and_then(|addrs| {
                            addrs.iter_locals().map(|locals| {
                                locals
                                    .filter(|s| **s != suffix)
                                    .flat_map(|s| s.iter())
                                    .cloned()
                                    .take(32)
                                    .collect::<Vec<_>>()
                            })
                        })
                    } else {
                        None
                    };

                    let peers = if let Some(remotes) = self
                        .peers
                        .get(&remote_cache)
                        .and_then(|addrs| addrs.remotes())
                    {
                        let num = cmp::min(
                            remotes.len(),
                            128 - local_peers.as_ref().map(|l| l.len()).unwrap_or_default(),
                        );
                        let mut buf = Vec::with_capacity(num * 6);

                        for addr in remotes.iter().filter(|addr| **addr != from).take(num) {
                            if let IpAddr::V4(ip) = addr.ip() {
                                buf.extend_from_slice(&ip.octets()[..]);
                                buf.extend_from_slice(&addr.port().to_be_bytes()[..]);
                            }
                        }
                        Some(buf)
                    } else {
                        None
                    };

                    let output = PeersOutput { peers, local_peers };
                    let mut buf = Vec::with_capacity(output.encoded_len());

                    // fits safe in vec
                    output.encode(&mut buf).unwrap();
                    query.value = Some(buf);
                    self.inner.reply_command(query.into());
                    return;
                }
                let _ = query.value.take();
                self.inner.reply_command(query.into());
            }
        }
    }

    /// Initiates an iterative query to the closest peers to lookup the topic.
    ///
    /// The result of the query is delivered in a
    /// [`HyperDhtEvent::LookupResult`].
    pub fn lookup(&mut self, target: IdBytes, commit: Commit) -> QueryId {
        let query_id = self.inner.query(
            Command::External(ExternalCommand(commands::LOOKUP)),
            target,
            None,
            commit,
        );
        self.queries.insert(
            query_id,
            QueryStreamType::Lookup(QueryStreamInner::new(target)),
        );
        query_id
    }

    /// Announce the topic to the closest peers
    ///
    /// Query result is a [`HyperDhtEvent::AnnounceResult`].
    pub fn announce(
        &mut self,
        target: IdBytes,
        key_pair: &Keypair,
        _relay_addresses: &[SocketAddr],
        opts: &QueryOpts2,
    ) -> QueryId {
        let _query_id = if opts.clear {
            self.lookup_and_unannounce(target, key_pair);
        } else {
            self.lookup(target, Commit::Custom(Default::default()));
        };
        // create Commit trait obj
        // register it with the query id
        todo!()
    }

    /// Initiates an iterative query to unannounce the topic to the closest
    /// peers.
    ///
    /// The result of the query is delivered in a
    /// [`HyperDhtEvent::UnAnnounceResult`].
    pub fn unannounce(&mut self, _opts: &QueryOpts) -> QueryId {
        todo!()
    }

    #[instrument(skip_all)]
    fn inject_response(&mut self, resp: Response) {
        trace!(
            cmd = display(resp.cmd),
            "Handle Response for custom command"
        );
        if let Some(query) = self.queries.get_mut(&resp.query) {
            match query {
                QueryStreamType::Announce(inner) | QueryStreamType::UnAnnounce(inner) => {
                    inner.inject_response(resp)
                }
                QueryStreamType::LookupAndUnannounce(_inner) => {
                    // do unannnounce request
                    // store request id to wait for request to finish
                    todo!()
                }
                QueryStreamType::Lookup(inner) => {
                    match LookupResponse::from_response(&resp) {
                        Ok(Some(evt)) => {
                            trace!("Decoded valid lookup response");
                            self.queued_events.push_back(evt.into());
                        }
                        Ok(None) => trace!("Lookup respones missing value field"),
                        Err(e) => error!(error = display(e), "Error decoding lookup response"),
                    }
                    inner.inject_response(resp)
                }
            }
        }
    }

    // A query was completed
    fn query_finished(&mut self, id: QueryId) {
        if let Some(query) = self.queries.remove(&id) {
            self.queued_events.push_back(query.finalize(id))
        } else {
            warn!(
                id = ?id,
                "Query with unknown id finished"
            );
        }
    }

    fn commit(&mut self, query: Arc<RwLock<Query>>, channel: mpsc::Sender<CommitMessage>) {
        let id = query.read().unwrap().id();
        let Some(qst) = self.queries.get_mut(&id) else {
            error!("Tried to commit with an unknown query id: [{id:?}]");
            panic!("Tried to commit with an unknown query id: [{id:?}]");
        };
        qst.commit(query, channel);
    }

    fn request_announce_or_unannounce(
        &mut self,
        keypair: &Keypair2,
        target: IdBytes,
        token: &[u8; 32],
        from: PeerId,
        relay_addresses: &[SocketAddr],
        namespace: &[u8; 32],
        cmd: ExternalCommand,
    ) -> Result<u16> {
        let announce = sign_announce_or_unannounce(
            keypair,
            target,
            token,
            &from.id.0,
            relay_addresses,
            namespace,
        )?;

        let mut value: Vec<u8> = vec![0u8; CompactEncodable::encoded_size(&announce).unwrap()];
        announce.encoded_bytes(&mut value).unwrap();
        let from_peer = Peer {
            id: Some(from.id.0),
            addr: from.addr,
            referrer: None,
        };
        Ok(self
            .inner
            .request(
                Command::External(cmd),
                Some(target),
                Some(value),
                from_peer,
                Some(*token),
            )
            .1)
    }

    #[allow(unused)] // TODO FIXME
    fn request_announce(
        &mut self,
        keypair: &Keypair2,
        target: IdBytes,
        token: &[u8; 32],
        from: PeerId,
        relay_addresses: &[SocketAddr],
    ) -> Result<u16> {
        self.request_announce_or_unannounce(
            keypair,
            target,
            token,
            from,
            relay_addresses,
            &crate::crypto::namespace::ANNOUNCE,
            ExternalCommand(commands::ANNOUNCE),
        )
    }

    #[allow(unused)] // TODO FIXME
    fn request_unannounce(
        &mut self,
        keypair: &Keypair2,
        target: IdBytes,
        token: &[u8; 32],
        from: PeerId,
    ) -> Result<u16> {
        self.request_announce_or_unannounce(
            keypair,
            target,
            token,
            from,
            &[],
            &crate::crypto::namespace::UNANNOUNCE,
            ExternalCommand(commands::UNANNOUNCE),
        )
    }
}

impl Stream for HyperDht {
    type Item = HyperDhtEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        loop {
            // Drain queued events first.
            if let Some(event) = pin.queued_events.pop_front() {
                return Poll::Ready(Some(event));
            }

            while let Poll::Ready(Some(ev)) = Stream::poll_next(Pin::new(&mut pin.inner), cx) {
                match ev {
                    RpcDhtEvent::RequestResult(Ok(RequestOk::CustomCommandRequest {
                        query,
                        request,
                        peer,
                    })) => pin.on_command(query, *request, peer),
                    RpcDhtEvent::ResponseResult(Ok(ResponseOk::Response(resp))) => {
                        pin.inject_response(resp)
                    }
                    RpcDhtEvent::Bootstrapped { stats } => {
                        return Poll::Ready(Some(HyperDhtEvent::Bootstrapped { stats }))
                    }
                    RpcDhtEvent::ReadyToCommit {
                        query,
                        tx_commit_messages,
                    } => {
                        pin.commit(query, tx_commit_messages);
                    }
                    RpcDhtEvent::QueryResult {
                        id,
                        cmd: _,
                        stats: _,
                    } => pin.query_finished(id),
                    _ => {}
                }
            }

            // No immediate event was produced as a result of the DHT.
            // If no new events have been queued either, signal `Pending` to
            // be polled again later.
            if pin.queued_events.is_empty() {
                return Poll::Pending;
            }
        }
    }
}

/// Events
///
/// The events produced by the `HyperDht` behaviour.
///
/// See [`HyperDht::poll`].
#[derive(Debug)]
pub enum HyperDhtEvent {
    /// The dht is now bootstrapped
    Bootstrapped {
        /// Execution statistics from the bootstrap query.
        stats: QueryStats,
    },
    /// The result of [`HyperDht::announce`].
    AnnounceResult(QueryResult),
    /// A response to part of a lookup query
    LookupResponse(LookupResponse),
    /// The result of [`HyperDht::lookup`].
    LookupResult(QueryResult),
    /// The result of [`HyperDht::unannounce`].
    UnAnnounceResult(QueryResult),
    /// Received a query with a custom command that is not automatically handled
    /// by the DHT
    CustomCommandQuery {
        /// The unknown command
        command: usize,
        /// The message we received from the peer.
        msg: Box<RequestMsgData>,
        /// The peer the message originated from.
        peer: Peer,
    },
}

/// Represents the response received from a peer
#[derive(Debug)]
pub struct PeerResponseItem<T: fmt::Debug> {
    /// Address of the peer this response came from
    pub peer: SocketAddr,
    /// The identifier of the `peer` if included in the response
    pub peer_id: Option<IdBytes>,
    /// The value the `peer` provided
    pub value: T,
}

/// Result of a [`HyperDht::lookup`] query.
#[derive(Debug, Clone)]
pub struct Lookup {
    /// The hash to lookup
    pub topic: IdBytes,
    /// The gathered responses
    pub peers: Vec<Peers>,
}

#[derive(Debug)]
pub struct QueryResult {
    pub topic: IdBytes,
    pub responses: Vec<Response>,
    pub query_id: QueryId,
}

impl QueryResult {
    fn new(query_id: &QueryId, qsi: QueryStreamInner) -> Self {
        Self {
            topic: qsi.topic,
            responses: qsi.peers,
            query_id: *query_id,
        }
    }
}

/// A Response to a query request from a peer
#[derive(Debug, Clone)]
pub struct Peers {
    /// The DHT node that is returning this data
    pub node: SocketAddr,
    /// The id of the `peer` if available
    pub peer_id: Option<IdBytes>,
    /// List of peers that announced the topic hash
    pub peers: Vec<SocketAddr>,
    /// List of LAN peers that announced the topic hash
    pub local_peers: Vec<SocketAddr>,
}

/// Type to keep track of the responses for queries in progress.
#[derive(Debug)]
#[allow(unused)]
enum QueryStreamType {
    LookupAndUnannounce(QueryStreamInner),
    Lookup(QueryStreamInner),
    Announce(QueryStreamInner),
    UnAnnounce(QueryStreamInner),
}

impl QueryStreamType {
    fn commit(&mut self, _query: Arc<RwLock<Query>>, _channel: mpsc::Sender<CommitMessage>) {
        match self {
            QueryStreamType::LookupAndUnannounce(_) => todo!(),
            QueryStreamType::Lookup(_) => todo!(),
            QueryStreamType::UnAnnounce(_) => todo!(),
            QueryStreamType::Announce(_inner) => todo!(),
        }
    }

    fn finalize(self, query_id: QueryId) -> HyperDhtEvent {
        match self {
            QueryStreamType::LookupAndUnannounce(_inner) => todo!(),
            QueryStreamType::Lookup(inner) => {
                HyperDhtEvent::LookupResult(QueryResult::new(&query_id, inner))
            }
            QueryStreamType::Announce(inner) => {
                HyperDhtEvent::AnnounceResult(QueryResult::new(&query_id, inner))
            }
            QueryStreamType::UnAnnounce(inner) => {
                HyperDhtEvent::UnAnnounceResult(QueryResult::new(&query_id, inner))
            }
        }
    }
}

#[derive(Debug)]
struct QueryStreamInner {
    topic: IdBytes,
    peers: Vec<Response>,
}

impl QueryStreamInner {
    #[allow(unused)] // TODO FIXME
    fn new(topic: IdBytes) -> Self {
        Self {
            topic,
            peers: Vec::new(),
        }
    }

    /// Store the decoded peers from the `Response` value
    #[instrument(skip_all)]
    fn inject_response(&mut self, resp: Response) {
        self.peers.push(resp);
    }
}
