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

use compact_encoding::{
    types::{write_array, CompactEncodable},
    EncodingError,
};
use crypto::{sign_announce, Keypair2};
use dht_rpc::{commit::CommitMessage, query::Query};
use ed25519_dalek::{Keypair, PublicKey};
use fnv::FnvHashMap;
use futures::{
    channel::mpsc::{self},
    task::{Context, Poll},
    Stream,
};
use libsodium_sys::crypto_sign_PUBLICKEYBYTES;
use prost::Message as ProstMessage;
use queries::QueryOpts as QueryOpts2;
use sha2::digest::generic_array::{typenum::U32, GenericArray};
use smallvec::alloc::collections::VecDeque;
use tokio::sync::oneshot::error::RecvError;
use tracing::{error, trace};

use crate::{
    dht_proto::{encode_input, PeersInput, PeersOutput},
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

    #[inline]
    pub fn encode_input(peers: &PeersInput) -> Vec<u8> {
        let mut buf = Vec::with_capacity(peers.encoded_len());
        // vec has sufficient capacity up to usize::MAX
        peers.encode(&mut buf).unwrap();
        buf
    }
}
mod cenc;
pub mod crypto;
pub mod lru;
mod queries;
pub mod store;
#[cfg(test)]
mod tests;

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
        self.inner.query(
            Command::External(ExternalCommand(commands::LOOKUP)),
            target,
            None,
            Commit::No,
        )
    }

    /// Do a LOOKUP and send an UNANNOUNCE to each node that replies
    pub fn lookup_and_unannounce(&mut self, target: IdBytes, _keypair: &Keypair) -> QueryId {
        let query_id = self.inner.query(
            Command::External(ExternalCommand(commands::LOOKUP)),
            target,
            None,
            Commit::Custom(Default::default()),
        );
        self.queries
            .insert(query_id, QueryStreamType::LookupAndUnannounce(todo!()));
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
            self.lookup(target, Commit::No);
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
    pub fn unannounce(&mut self, opts: impl Into<QueryOpts>) -> QueryId {
        let opts = opts.into();

        let peers = PeersInput {
            port: opts.port,
            local_address: opts.local_addr_encoded(),
            unannounce: Some(true),
        };
        let _buf = encode_input(&peers);

        /*
        let id = self
            .inner
            .update(PEERS_CMD, kbucket::Key::new(opts.topic.clone()), Some(buf));
        self.queries.insert(
            id,
            QueryStreamType::UnAnnounce(QueryStreamInner::new(opts.topic, opts.local_addr)),
        );
        id
            */
        todo!()
    }

    fn inject_response(&mut self, resp: Response) {
        if let Some(query) = self.queries.get_mut(&resp.query) {
            match query {
                QueryStreamType::LookUp(inner)
                | QueryStreamType::Announce(inner)
                | QueryStreamType::UnAnnounce(inner) => inner.inject_response(resp),
                QueryStreamType::LookupAndUnannounce(_inner) => {
                    // do unannnounce request
                    // store request id to wait for request to finish
                    todo!()
                }
            }
        }
    }

    // A query was completed
    fn query_finished(&mut self, id: QueryId) {
        if let Some(query) = self.queries.remove(&id) {
            self.queued_events.push_back(query.finalize(id))
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

    #[allow(unused)] // TODO FIXME
    fn request_announce(
        &mut self,
        keypair: &Keypair2,
        target: IdBytes,
        token: &[u8; 32],
        from: PeerId,
        relay_addresses: &[SocketAddr],
    ) -> Result<u16> {
        let announce = sign_announce(keypair, target, token, &from.id.0, relay_addresses)?;

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
                Command::External(ExternalCommand(commands::ANNOUNCE)),
                Some(target),
                Some(value),
                from_peer,
                Some(*token),
            )
            .1)
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
                trace!("DHT event {:?}", ev);
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

/// Determines what to announce or query from the DHT
#[derive(Debug, Clone, PartialEq)]
pub struct QueryOpts {
    /// The topic to announce
    pub topic: IdBytes,
    /// Explicitly set the port you want to announce. Per default the UDP socket
    /// port is announced.
    pub port: Option<u32>,
    /// Optionally announce a LAN address as well. Only people with the same
    /// public IP as you will get these when doing a lookup
    pub local_addr: Option<SocketAddr>,
}

impl QueryOpts {
    /// Create a opts with only a topic
    pub fn new(topic: impl Into<IdBytes>) -> Self {
        Self {
            topic: topic.into(),
            port: None,
            local_addr: None,
        }
    }

    /// Create new opts with a topic and a port
    pub fn with_port(topic: impl Into<IdBytes>, port: u32) -> Self {
        Self {
            topic: topic.into(),
            port: Some(port),
            local_addr: None,
        }
    }

    /// Set the port to announce
    pub fn port(mut self, port: u32) -> Self {
        self.port = Some(port);
        self
    }

    /// The local addresses as encoded payload
    fn local_addr_encoded(&self) -> Option<Vec<u8>> {
        self.local_addr.as_ref().map(|addr| addr.encode())
    }

    /// Set Local addresses to announce
    pub fn local_addr(mut self, local_addr: impl ToSocketAddrs) -> Self {
        self.local_addr = local_addr
            .to_socket_addrs()
            .ok()
            .and_then(|mut iter| iter.next());
        self
    }
}

impl From<&PublicKey> for QueryOpts {
    fn from(key: &PublicKey) -> Self {
        Self {
            topic: IdBytes(*key.as_bytes()),
            port: None,
            local_addr: None,
        }
    }
}

impl From<&GenericArray<u8, U32>> for QueryOpts {
    fn from(digest: &GenericArray<u8, U32>) -> Self {
        Self {
            topic: digest.as_slice().try_into().expect("Wrong length"),
            port: None,
            local_addr: None,
        }
    }
}

impl From<IdBytes> for QueryOpts {
    fn from(topic: IdBytes) -> Self {
        Self {
            topic,
            port: None,
            local_addr: None,
        }
    }
}

impl TryFrom<&[u8]> for QueryOpts {
    type Error = std::array::TryFromSliceError;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            topic: value.try_into()?,
            port: None,
            local_addr: None,
        })
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
    AnnounceResult {
        /// The peers that successfully received the announcement
        peers: Vec<Peers>,
        /// The announced topic.
        topic: IdBytes,
        /// Tracking id of the query
        query_id: QueryId,
    },
    /// The result of [`HyperDht::lookup`].
    LookupResult {
        /// All responses
        lookup: Lookup,
        /// /// Tracking id of the query
        query_id: QueryId,
    },
    /// The result of [`HyperDht::unannounce`].
    UnAnnounceResult {
        /// The peers that received the un announce message
        peers: Vec<Peers>,
        /// The topic that was un announced
        topic: IdBytes,
        /// Tracking id of the query
        query_id: QueryId,
    },
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

impl Lookup {
    /// Returns an iterator over all the nodes that sent data for this look
    pub fn origins(&self) -> impl Iterator<Item = (&SocketAddr, Option<&IdBytes>)> {
        self.peers
            .iter()
            .map(|peer| (&peer.node, peer.peer_id.as_ref()))
    }

    /// Returns an iterator over all remote peers that announced the topic hash
    pub fn remotes(&self) -> impl Iterator<Item = &SocketAddr> {
        self.peers.iter().flat_map(|peer| peer.peers.iter())
    }

    /// Returns an iterator over all LAN peers that announced the topic hash
    pub fn locals(&self) -> impl Iterator<Item = &SocketAddr> {
        self.peers.iter().flat_map(|peer| peer.local_peers.iter())
    }

    /// Returns an iterator over all peers (remote and LAN) that announced the
    /// topic hash.
    pub fn all_peers(&self) -> impl Iterator<Item = &SocketAddr> {
        self.peers
            .iter()
            .flat_map(|peer| peer.peers.iter().chain(peer.local_peers.iter()))
    }

    /// Amount peers matched the lookup
    #[inline]
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Returns `true` if the lookup contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
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
    LookUp(QueryStreamInner),
    Announce(QueryStreamInner),
    UnAnnounce(QueryStreamInner),
}

impl QueryStreamType {
    fn commit(&mut self, _query: Arc<RwLock<Query>>, _channel: mpsc::Sender<CommitMessage>) {
        match self {
            QueryStreamType::LookupAndUnannounce(_) => todo!(),
            QueryStreamType::LookUp(_) => todo!(),
            QueryStreamType::UnAnnounce(_) => todo!(),
            QueryStreamType::Announce(_inner) => todo!(),
        }
    }
    fn finalize(self, query_id: QueryId) -> HyperDhtEvent {
        match self {
            QueryStreamType::LookupAndUnannounce(_inner) => todo!(),
            QueryStreamType::LookUp(inner) => HyperDhtEvent::LookupResult {
                lookup: Lookup {
                    peers: inner.responses,
                    topic: inner.topic,
                },
                query_id,
            },
            QueryStreamType::Announce(inner) => HyperDhtEvent::AnnounceResult {
                peers: inner.responses,
                topic: inner.topic,
                query_id,
            },
            QueryStreamType::UnAnnounce(inner) => HyperDhtEvent::UnAnnounceResult {
                peers: inner.responses,
                topic: inner.topic,
                query_id,
            },
        }
    }
}

#[derive(Debug)]
struct QueryStreamInner {
    topic: IdBytes,
    responses: Vec<Peers>,
    local_address: Option<SocketAddr>,
}

impl QueryStreamInner {
    #[allow(unused)] // TODO FIXME
    fn new(topic: IdBytes, local_address: Option<SocketAddr>) -> Self {
        Self {
            topic,
            responses: Vec::new(),
            local_address,
        }
    }

    /// Store the decoded peers from the `Response` value
    fn inject_response(&mut self, resp: Response) {
        if let Some(val) = resp
            .value
            .as_ref()
            .and_then(|val| PeersOutput::decode(val.as_slice()).ok())
        {
            let peers = val.peers.as_ref().map(decode_peers).unwrap_or_default();

            let local_peers = val
                .local_peers
                .as_ref()
                .and_then(|buf| {
                    if let Some(SocketAddr::V4(addr)) = self.local_address {
                        Some(decode_local_peers(&addr, buf))
                    } else {
                        None
                    }
                })
                .unwrap_or_default();

            if peers.is_empty() && local_peers.is_empty() {
                return;
            }
            self.responses.push(Peers {
                node: resp.peer,
                peer_id: resp.peer_id,
                peers,
                local_peers,
            })
        }
    }
}
