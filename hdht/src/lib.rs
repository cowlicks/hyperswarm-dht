//! Rust Implementation of the hyperswarm DHT
#![warn(rust_2018_idioms)]
#![allow(unreachable_code)]
#![deny(clippy::enum_glob_use)]

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

use commands::{ANNOUNCE, LOOKUP};
use compact_encoding::{types::CompactEncodable, EncodingError};
use crypto::{sign_announce_or_unannounce, Keypair2, PublicKey2};
use dht_rpc::{
    commit::{CommitMessage, CommitRequestParams, Progress},
    io::InResponse,
    query::Query,
    Tid,
};
use fnv::FnvHashMap;
use futures::{
    channel::mpsc::{self},
    task::{Context, Poll},
    Stream,
};
use prost::Message as ProstMessage;
use queries::{
    AnnounceInner, LookupResponse, UnannounceInner, UnannounceRequest, UnannounceResult,
};
use smallvec::alloc::collections::VecDeque;
use tokio::sync::oneshot::error::RecvError;
use tracing::{debug, error, instrument, trace, warn};

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
    ResponseOk, RpcDht, RpcDhtBuilderError, RpcDhtEvent,
};

mod dht_proto {
    include!(concat!(env!("OUT_DIR"), "/dht_pb.rs"));
}
pub mod cenc;
pub mod crypto;
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
    #[error("Invalid Signature")]
    InvalidSignature(i32),
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

    pub fn find_peer(&mut self, pub_key: PublicKey2) -> QueryId {
        let target = IdBytes(generic_hash(&*pub_key));

        self.inner.query(
            Command::External(ExternalCommand(commands::FIND_PEER)),
            target,
            None,
            Commit::No,
            false,
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

                    let remote_cache = CacheKey::Remote(query.target);

                    let local_cache = peer.local_address.as_ref().and_then(|l| {
                        if l.len() == 6 {
                            let prefix: [u8; 2] = l[0..2].try_into().unwrap();
                            let suffix: [u8; 4] = l[2..].try_into().unwrap();
                            Some((
                                CacheKey::Local {
                                    id: query.target,
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
            false,
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
        key_pair: &Keypair2,
        _relay_addresses: &[SocketAddr],
    ) -> QueryId {
        let qid = self.inner.query(
            Command::External(ExternalCommand(commands::LOOKUP)),
            target,
            None,
            Commit::Custom(Progress::default()),
            true,
        );
        self.queries.insert(
            qid,
            QueryStreamType::Announce(AnnounceInner::new(target, key_pair.clone())),
        );
        qid
    }

    /// Initiates an iterative query to unannounce the topic to the closest
    /// peers.
    ///
    /// The result of the query is delivered in a
    /// [`HyperDhtEvent::UnAnnounceResult`].
    pub fn unannounce(&mut self, target: IdBytes, key_pair: &Keypair2) -> QueryId {
        let qid = self.lookup(target, Commit::No);
        self.queries.insert(
            qid,
            QueryStreamType::UnAnnounce(UnannounceInner::new(target, key_pair.clone())),
        );
        qid
    }

    #[instrument(skip_all)]
    fn inject_response(&mut self, resp: Arc<InResponse>) {
        trace!(
            cmd = display(resp.cmd()),
            "Handle Response for custom command"
        );
        // Holding `&mut query` here prevents us from calling self.request_unannounce
        // maybe instead we return something from the block to do the msg that we want?
        // however I want to pass in an id from the request.
        // I really just want a mut ref to the query, but
        let request = {
            if let Some((query, qid)) = resp
                .query_id
                .and_then(|qid| self.queries.get_mut(&qid).map(|q| (q, qid)))
            {
                match query {
                    QueryStreamType::Announce(inner) => {
                        inner.inject_response(resp);
                        None
                    }
                    QueryStreamType::UnAnnounce(inner) => {
                        // For responses to Lookup requests send an unannounce requests
                        if let (Some(token), Some(id), Command::External(ExternalCommand(LOOKUP))) =
                            (&resp.response.token, &resp.valid_peer_id(), resp.cmd())
                        {
                            let destination = PeerId {
                                addr: resp.peer.addr,
                                id: *id,
                            };

                            let req = UnannounceRequest {
                                tid: self.inner.new_tid(),
                                query_id: qid,
                                keypair: inner.keypair.clone(),
                                topic: inner.topic,
                                token: *token,
                                destination,
                            };

                            inner.inject_response(resp, req.tid);
                            warn!("GOOD unannounce sending unannounce");
                            Some(req)
                        } else {
                            warn!(
                                resp.tid = resp.response.tid,
                                resp.query_id = display(qid),
                                resp.token = debug(resp.response.token),
                                resp.peer_id = debug(resp.valid_peer_id()),
                                resp.cmd = display(resp.cmd()),
                                "Other kind of response to lookup query"
                            );
                            None
                        }
                    }
                    QueryStreamType::LookupAndUnannounce(_inner) => {
                        // do unannnounce request
                        // store request id to wait for request to finish
                        todo!()
                    }
                    QueryStreamType::Lookup(inner) => {
                        match LookupResponse::from_response(resp.clone()) {
                            Ok(Some(evt)) => {
                                trace!("Decoded valid lookup response");
                                self.queued_events.push_back(evt.into());
                            }
                            Ok(None) => trace!("Lookup respones missing value field"),
                            Err(e) => error!(error = display(e), "Error decoding lookup response"),
                        }
                        inner.inject_response(resp);
                        None
                    }
                }
            } else {
                None
            }
        };
        if let Some(req) = request {
            let qid = req.query_id;
            self.inner.send_request((Some(qid), req.into()))
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
        destination: PeerId,
        relay_addresses: &[SocketAddr],
        namespace: &[u8; 32],
        cmd: ExternalCommand,
    ) -> Tid {
        let value = request_announce_or_unannounce_value(
            keypair,
            target,
            token,
            destination.id,
            relay_addresses,
            namespace,
        );

        let from_peer = Peer {
            id: Some(destination.id.0),
            addr: destination.addr,
            referrer: None,
        };

        self.inner.request(
            Command::External(cmd),
            Some(target),
            Some(value),
            from_peer,
            Some(*token),
        )
    }

    #[allow(unused)] // TODO FIXME
    fn request_announce(
        &mut self,
        keypair: &Keypair2,
        target: IdBytes,
        token: &[u8; 32],
        destination: PeerId,
        relay_addresses: &[SocketAddr],
    ) -> Tid {
        // TODO rm result
        self.request_announce_or_unannounce(
            keypair,
            target,
            token,
            destination,
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
        destination: PeerId,
    ) -> Tid {
        self.request_announce_or_unannounce(
            keypair,
            target,
            token,
            destination,
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
// TODO should this be refactored into...
// - enum {
//   Response(ResponseKind) / Ann, Look, etc
//   Result(ResultKind) / Ann, Look, etc
//   ...
// }
// - enum {
//   Announce(Ann/Result/Resp)
//   Lookup(Ann/Result/Resp)
// }
// ... maybe for now just make it flat
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
    UnAnnounceResult(UnannounceResult),
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

impl HyperDhtEvent {
    pub fn kind(&self) -> &'static str {
        match &self {
            HyperDhtEvent::Bootstrapped { .. } => "Bootstrapped",
            HyperDhtEvent::AnnounceResult(_) => "AnnounceResult",
            HyperDhtEvent::LookupResponse(_) => "LookupResponse",
            HyperDhtEvent::LookupResult(_) => "LookupResult",
            HyperDhtEvent::UnAnnounceResult(_) => "UnAnnounceResult",
            HyperDhtEvent::CustomCommandQuery { .. } => "CustomCommandQuery",
        }
    }
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
    pub responses: Vec<Arc<InResponse>>,
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
    Announce(AnnounceInner),
    UnAnnounce(UnannounceInner),
}

impl QueryStreamType {
    fn commit(&mut self, query: Arc<RwLock<Query>>, mut channel: mpsc::Sender<CommitMessage>) {
        match self {
            QueryStreamType::LookupAndUnannounce(_) => todo!(),
            QueryStreamType::Lookup(_) => todo!(),
            QueryStreamType::UnAnnounce(_) => todo!(),
            QueryStreamType::Announce(inner) => {
                let q = query.read().unwrap();
                // TODO UGLY
                warn!("# closest replies = [{}]", q.closest_replies.len());
                for cr in q.closest_replies.iter() {
                    let Some(pid) = cr.request.to.id else {
                        // refactor this to be handled in the type system
                        warn!("closest_replies peer without id.. Should not happen");
                        continue;
                    };
                    trace!(
                        "Sending commit to peer.id = [{:?}]",
                        Into::<IdBytes>::into(pid)
                    );
                    channel
                        .try_send(CommitMessage::Send(CommitRequestParams {
                            command: Command::External(ExternalCommand(ANNOUNCE)),
                            target: Some(inner.topic),
                            value: Some(request_announce_or_unannounce_value(
                                &inner.keypair,
                                inner.topic,
                                &cr.response.token.expect("todo"),
                                pid.into(),
                                &[],
                                &crate::crypto::namespace::ANNOUNCE,
                            )),
                            peer: cr.peer.addr,
                            query_id: q.id,
                            token: cr.response.token.expect("TODO"),
                        }))
                        .expect("TODO");
                }

                debug!("Emit CommitMessage::Done for query.id = {}", q.id);
                channel.try_send(CommitMessage::Done).unwrap();
            }
        }
    }

    fn finalize(self, query_id: QueryId) -> HyperDhtEvent {
        match self {
            QueryStreamType::LookupAndUnannounce(_inner) => todo!(),
            QueryStreamType::Lookup(inner) => {
                HyperDhtEvent::LookupResult(QueryResult::new(&query_id, inner))
            }
            QueryStreamType::Announce(inner) => HyperDhtEvent::AnnounceResult(QueryResult {
                topic: inner.topic,
                responses: inner.responses,
                query_id,
            }),
            QueryStreamType::UnAnnounce(_inner) => {
                HyperDhtEvent::UnAnnounceResult(UnannounceResult {})
            }
        }
    }
}

#[derive(Debug)]
struct QueryStreamInner {
    topic: IdBytes,
    peers: Vec<Arc<InResponse>>,
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
    fn inject_response(&mut self, resp: Arc<InResponse>) {
        self.peers.push(resp);
    }
}

pub fn request_announce_or_unannounce_value(
    keypair: &Keypair2,
    target: IdBytes,
    token: &[u8; 32],
    from: IdBytes,
    relay_addresses: &[SocketAddr],
    namespace: &[u8; 32],
) -> Vec<u8> {
    let announce =
        sign_announce_or_unannounce(keypair, target, token, &from.0, relay_addresses, namespace);
    announce
        .to_bytes()
        .expect("known to succeed for all `Announce` values")
}
