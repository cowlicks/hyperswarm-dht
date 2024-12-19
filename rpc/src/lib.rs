//! Rust Implementation of the hyperswarm DHT
#![warn(rust_2018_idioms)]
#![allow(unreachable_code)]

use compact_encoding::EncodingError;
use kbucket::{distance, Distance};
use tokio::sync::oneshot::error::RecvError;

use ed25519_dalek::{PublicKey, PUBLIC_KEY_LENGTH};
use futures::Stream;
use query::{CommandQueryResponse, Commit};
use std::{
    array::TryFromSliceError,
    borrow::Borrow,
    collections::{HashSet, VecDeque},
    convert::{TryFrom, TryInto},
    fmt::Display,
    net::{AddrParseError, SocketAddr, ToSocketAddrs},
    ops::Deref,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
    time::Duration,
};
use tracing::{debug, trace, warn};
use wasm_timer::Instant;

use rand::{
    rngs::{OsRng, StdRng},
    RngCore, SeedableRng,
};

use crate::{
    cenc::generic_hash,
    jobs::PeriodicJob,
    kbucket::{Entry, EntryView, InsertResult, KBucketsTable, NodeStatus, K_VALUE},
    query::QueryId,
    util::pretty_bytes,
};

pub use self::message::{ReplyMsgData, RequestMsgData};
use self::{
    io::{IoConfig, IoHandler, IoHandlerEvent},
    message::valid_id_bytes,
    mslave::Master,
    query::{
        table::PeerState, CommandQuery, Query, QueryConfig, QueryEvent, QueryPool, QueryPoolState,
        QueryStats,
    },
    stream::MessageDataStream,
};

pub mod cenc;
pub mod constants;
pub mod io;
mod jobs;
pub mod kbucket;
mod message;
mod mslave;
pub mod peers;
pub mod query;
mod stream;
mod util;

#[cfg(test)]
mod s_test;
#[cfg(test)]
pub mod test;

/// The publicly available hyperswarm DHT addresses
pub const DEFAULT_BOOTSTRAP: [&str; 3] = [
    "node1.hyperdht.org:49737",
    "node2.hyperdht.org:49737",
    "node3.hyperdht.org:49737",
];

/// The command identifier for `Mutable` storage
pub const MUTABLE_STORE_CMD: usize = 1;
/// The command identifier for immutable storage
pub const IMMUTABLE_STORE_CMD: usize = 2;
/// The command identifier to (un)announce/lookup peers
pub const PEERS_CMD: usize = 3;

#[derive(Debug, thiserror::Error)]
pub enum Error {
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

/// TODO in js this is de/encoded with c.uint which is for a variable sized unsigned integer.
// but it is always one byte. We use a u8 instead of a usize here. So there is a limit on 256
// commands.
#[derive(Copy, Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum InternalCommand {
    Ping = 0,
    PingNat,
    FindNode,
    DownHint,
}

impl Display for InternalCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use InternalCommand::*;
        write!(f, "COMMAND")?;
        match self {
            Ping => write!(f, "::Ping"),
            PingNat => write!(f, "::PingNat"),
            FindNode => write!(f, "::FindNode"),
            DownHint => write!(f, "::DownHint"),
        }
    }
}

impl From<InternalCommand> for Command {
    fn from(value: InternalCommand) -> Self {
        Command::Internal(value)
    }
}

#[derive(Copy, Debug, Clone, PartialEq)]
pub struct ExternalCommand(pub usize);

/// TODO This is encoded as u8 which might not always be true
#[derive(Copy, Debug, Clone, PartialEq)]
pub enum Command {
    Internal(InternalCommand),
    External(ExternalCommand),
}

impl Command {
    fn encode(&self) -> u8 {
        match &self {
            Command::Internal(cmd) => *cmd as u8,
            Command::External(ExternalCommand(cmd)) => *cmd as u8,
        }
    }
}

impl TryFrom<u8> for InternalCommand {
    type Error = crate::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use InternalCommand::*;
        Ok(match value {
            0 => Ping,
            1 => PingNat,
            2 => FindNode,
            3 => DownHint,
            x => return Err(crate::Error::InvalidRpcCommand(x)),
        })
    }
}

fn thirty_two_random_bytes() -> [u8; 32] {
    let mut buff = [0; 32];
    let mut rng = StdRng::from_rng(OsRng).unwrap();
    rng.fill_bytes(&mut buff);
    buff
}

pub(crate) type QueryAndTid = (Option<QueryId>, u16);

#[derive(Debug, derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct RpcDht {
    // TODO use message passing to update id's in IoHandler
    #[builder(default = "Master::new(IdBytes::from(thirty_two_random_bytes()))")]
    pub id: Master<IdBytes>,
    ephemeral: bool,
    pub(crate) kbuckets: KBucketsTable<Node>,
    io: IoHandler,
    bootstrap_job: PeriodicJob,
    ping_job: PeriodicJob,
    /// The currently active (i.e. in-progress) queries.
    queries: QueryPool,
    /// Custom commands
    commands: HashSet<usize>,
    /// Queued events to return when being polled.
    queued_events: VecDeque<RpcDhtEvent>,
    #[builder(field(ty = "Vec<SocketAddr>"))]
    bootstrap_nodes: Vec<SocketAddr>,
    bootstrapped: bool,
    down_hints_in_progress: Vec<(u16, IdBytes, Instant)>,
}

#[derive(Debug)]
pub struct DhtConfig {
    pub kbucket_pending_timeout: Duration,
    pub local_id: Option<[u8; 32]>,
    pub commands: HashSet<usize>,
    pub query_config: QueryConfig,
    pub io_config: IoConfig,
    pub bootstrap_interval: Duration,
    pub ping_interval: Duration,
    #[allow(unused)] // FIXME bg: why never read?
    pub connection_idle_timeout: Duration,
    pub ephemeral: bool,
    pub adaptive: bool,
    pub bootstrap_nodes: Vec<SocketAddr>,
    pub socket: Option<MessageDataStream>,
}

impl Default for DhtConfig {
    fn default() -> Self {
        DhtConfig {
            kbucket_pending_timeout: Duration::from_secs(60),
            local_id: None,
            commands: Default::default(),
            query_config: Default::default(),
            ping_interval: Duration::from_secs(40),
            bootstrap_interval: Duration::from_secs(320),
            connection_idle_timeout: Duration::from_secs(10),
            ephemeral: false,
            adaptive: false,
            bootstrap_nodes: Vec::new(),
            socket: None,
            io_config: Default::default(),
        }
    }
}

impl DhtConfig {
    /// Create a new UDP socket and attempt to bind it to the addr provided.
    pub fn bind<A: ToSocketAddrs>(mut self, addr: A) -> crate::Result<Self> {
        self.socket = Some(MessageDataStream::bind(addr)?);
        Ok(self)
    }
    pub fn add_bootstrap_node<A: Into<SocketAddr>>(mut self, addr: A) -> Self {
        self.bootstrap_nodes.push(addr.into());
        self
    }
    pub fn empty_bootstrap_nodes(mut self) -> Self {
        self.bootstrap_nodes = vec![];
        self
    }
    /// Set the nodes to bootstrap from
    pub fn set_bootstrap_nodes<T: ToSocketAddrs>(mut self, addresses: &[T]) -> Self {
        let mut bootstrap_nodes = vec![];

        for addrs in addresses {
            if let Ok(addrs) = addrs.to_socket_addrs() {
                for addr in addrs {
                    bootstrap_nodes.push(addr)
                }
            }
        }
        self.bootstrap_nodes = bootstrap_nodes;
        self
    }

    /// Register all commands to listen to.
    pub fn register_commands(mut self, cmds: &[usize]) -> Self {
        for cmd in cmds {
            self.commands.insert(*cmd);
        }
        self
    }

    /// Set ephemeral: true so other peers do not add us to the peer list,
    /// simply bootstrap.
    ///
    /// An ephemeral dht node won't expose its id to remote peers, hence being
    /// ignored.
    pub fn ephemeral(mut self) -> Self {
        self.ephemeral = true;
        self
    }

    pub fn set_ephemeral(mut self, ephemeral: bool) -> Self {
        self.ephemeral = ephemeral;
        self
    }
}

impl RpcDht {
    pub async fn with_config(config: DhtConfig) -> crate::Result<Self> {
        let bites = config.local_id.unwrap_or_else(thirty_two_random_bytes);
        let id_bytes = IdBytes::from(bites);
        let local_id = id_bytes.clone();
        let id = Master::new(local_id.clone());

        let socket = config
            .socket
            .map(Result::Ok)
            .unwrap_or_else(MessageDataStream::defualt_bind)?;

        let io = IoHandler::new(id.view(), socket, config.io_config);

        let mut dht = Self {
            id,
            ephemeral: config.ephemeral,
            kbuckets: KBucketsTable::new(local_id.clone(), config.kbucket_pending_timeout),
            io,
            bootstrap_job: PeriodicJob::new(config.bootstrap_interval),
            ping_job: PeriodicJob::new(config.ping_interval),
            queries: QueryPool::new(local_id, config.query_config),
            commands: config.commands,
            queued_events: Default::default(),
            bootstrap_nodes: config.bootstrap_nodes,
            bootstrapped: false,
            down_hints_in_progress: Vec::new(),
        };

        dht.bootstrap();
        Ok(dht)
    }

    pub fn is_ephemeral(&self) -> bool {
        self.ephemeral
    }

    /// Returns the local address that this listener is bound to.
    pub fn local_addr(&self) -> crate::Result<SocketAddr> {
        self.io.local_addr()
    }
    /// Returns the id used to identify this node.
    pub fn local_id(&self) -> &IdBytes {
        self.id.get_ref()
    }

    pub fn bootstrap(&mut self) {
        if !self.bootstrap_nodes.is_empty() {
            let target = self.id.get().clone();
            let peers = self
                .kbuckets
                .closest(&target)
                .take(usize::from(K_VALUE))
                .map(|e| PeerId::new(e.node.value.addr, e.node.key.clone()))
                .collect::<Vec<_>>();
            let bootstrap_nodes: Vec<Peer> = self.bootstrap_nodes.iter().map(Peer::from).collect();
            self.queries.bootstrap(target, peers, bootstrap_nodes);
        } else if !self.bootstrapped {
            self.queued_events.push_back(RpcDhtEvent::Bootstrapped {
                stats: QueryStats::empty(),
            });
            self.bootstrapped = true;
        }
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped
    }

    pub fn query(
        &mut self,
        cmd: Command,
        target: IdBytes,
        value: Option<Vec<u8>>,
        commit: Commit,
    ) -> QueryId {
        self.run_command(cmd, target, value, commit)
    }

    fn run_command(
        &mut self,
        cmd: Command,
        target: IdBytes,
        value: Option<Vec<u8>>,
        commit: Commit,
    ) -> QueryId {
        let peers = self
            .kbuckets
            .closest(&target)
            .take(usize::from(K_VALUE))
            .map(|e| PeerId::new(e.node.value.addr, e.node.key.clone()))
            .collect::<Vec<_>>();

        let bootstrap_nodes: Vec<Peer> = self.bootstrap_nodes.iter().map(Peer::from).collect();
        self.queries
            .add_stream(cmd, peers, target, value, bootstrap_nodes, commit)
    }

    pub fn ping(&mut self, peer: &Peer) -> QueryAndTid {
        self.io.queue_send_request(
            Command::Internal(InternalCommand::Ping),
            None,
            None,
            peer.clone(),
            None,
            None,
        )
    }

    fn ping_some(&mut self) -> Vec<QueryAndTid> {
        let cnt = if self.queries.len() > 2 { 3 } else { 5 };
        let now = Instant::now();
        let mut out = vec![];
        let ping_interval = self.ping_job.interval;
        for peer in self
            .kbuckets
            .iter()
            .filter_map(|entry| {
                if now > entry.node.value.last_seen + ping_interval {
                    Some(Peer::from(entry.node.value.addr))
                } else {
                    None
                }
            })
            .take(cnt)
            .collect::<Vec<_>>()
        {
            out.push(self.ping(&peer));
        }
        out
    }

    /// Handle the event generated from the underlying IO
    fn inject_event(&mut self, event: IoHandlerEvent) {
        match event {
            IoHandlerEvent::OutResponse { .. } => {}
            IoHandlerEvent::OutSocketErr { .. } => {}
            IoHandlerEvent::InRequest { message, peer } => {
                self.on_request(message, peer);
            }
            IoHandlerEvent::InMessageErr { .. } => {}
            IoHandlerEvent::InSocketErr { .. } => {}
            IoHandlerEvent::InResponseBadRequestId { .. } => {
                // received a response that did not match any issued requests
                //self.remove_node(&peer);
                todo!()
            }
            IoHandlerEvent::OutRequest { .. } => {
                // sent a request
            }
            IoHandlerEvent::InResponse {
                req,
                resp,
                peer,
                query_id,
            } => {
                self.on_response(req, resp, peer, query_id);
            }
            IoHandlerEvent::RequestTimeout { .. } => {
                todo!()
            }
        }
    }

    /// Process a response.
    fn on_response(
        &mut self,
        req: Box<RequestMsgData>,
        resp: ReplyMsgData,
        peer: Peer,
        query_id: Option<QueryId>,
    ) {
        if let Some(id) = valid_id_bytes(resp.id) {
            self.add_node(id, peer.clone(), None, Some(SocketAddr::from(&resp.to)));
        }
        match query_id {
            Some(query_id) => {
                if let Some(query) = self.queries.get_mut(&query_id) {
                    if let Some(resp) = query.inject_response(resp, peer) {
                        self.queued_events
                            .push_back(RpcDhtEvent::ResponseResult(Ok(ResponseOk::Response(resp))))
                    }
                } else {
                    debug!("Recieved response for missing query with id: {query_id:?}. It could have been removed already");
                }
            }
            None => {
                if matches!(req.command, Command::Internal(InternalCommand::Ping)) {
                    if query_id.is_some() {
                        panic!("Pings should not have a QueryId");
                    }
                    self.on_pong(resp, peer);
                    return;
                }
                panic!("TODO");
            }
        }
    }

    /// Handle an incoming request.
    ///
    /// Eventually send a response.
    fn on_request(&mut self, msg: RequestMsgData, peer: Peer) {
        if let Some(id) = valid_id_bytes(msg.id) {
            self.add_node(id, peer.clone(), None, Some(SocketAddr::from(&msg.to)));
        }

        match msg.command {
            Command::Internal(cmd) => match cmd {
                InternalCommand::Ping => self.on_ping(msg, &peer),
                InternalCommand::FindNode => self.on_find_node(msg, peer),
                InternalCommand::PingNat => self.on_ping_nat(msg, peer),
                InternalCommand::DownHint => self.on_down_hint(msg, peer),
            },
            Command::External(_cmd) => todo!(),
        }
    }

    fn add_node(
        &mut self,
        id: IdBytes,
        peer: Peer,
        roundtrip_token: Option<Vec<u8>>,
        to: Option<SocketAddr>,
    ) {
        match self.kbuckets.entry(&id) {
            Entry::Present(mut entry, _) => {
                entry.value().last_seen = Instant::now();
            }
            Entry::Pending(mut entry, _) => {
                let n = entry.value();
                n.addr = peer.addr;
                n.last_seen = Instant::now();
            }
            Entry::Absent(entry) => {
                let node = Node {
                    addr: peer.addr,
                    roundtrip_token,
                    to,
                    last_seen: Instant::now(),
                    referrers: vec![],
                    down_hint: false,
                    last_pinged: None,
                };

                use InsertResult::*;
                match entry.insert(node, NodeStatus::Connected) {
                    Inserted => {
                        self.queued_events.push_back(RpcDhtEvent::RoutingUpdated {
                            peer,
                            old_peer: None,
                        });
                    }
                    Full => {
                        debug!("Bucket full. Peer not added to routing table: {:?}", peer)
                    }
                    Pending { disconnected: _ } => {

                        // TODO dial remote
                    }
                }
            }
            Entry::SelfEntry => {}
        }
    }
    pub fn reply_command(&mut self, resp: CommandQueryResponse) {
        self.io.reply(resp.msg)
    }

    fn reply(
        &mut self,
        error: usize,
        value: Option<Vec<u8>>,
        token: Option<[u8; 32]>,
        has_closer_nodes: bool,
        request: RequestMsgData,
        peer: &Peer,
    ) {
        let t: Vec<Peer> = match (has_closer_nodes, request.target) {
            (true, Some(t)) => self
                .kbuckets
                .closest(&IdBytes::from(t))
                .take(usize::from(K_VALUE))
                .map(|entry| Peer::from(entry.node.value.addr))
                .collect(),
            _ => vec![],
        };
        let msg = ReplyMsgData {
            tid: request.tid,
            to: peer.clone(),
            id: None,
            token,
            closer_nodes: t,
            error,
            value,
        };
        self.io.reply(msg)
    }

    /// Handle a ping request
    fn on_ping(&mut self, msg: RequestMsgData, peer: &Peer /* peer who sent ping */) {
        let msg = ReplyMsgData {
            tid: msg.tid,
            to: peer.clone(),
            id: (!self.ephemeral).then(|| self.id.get().0),
            token: self.io.token(peer, 1).ok(),
            closer_nodes: vec![],
            error: 0,
            value: None,
        };
        self.io.reply(msg)
    }

    /// Handle an incoming find peers request.
    ///
    /// Reply only if the remote provided a target to get the closest nodes for.
    fn on_find_node(&mut self, request: RequestMsgData, peer: Peer) {
        let closer_nodes: Vec<Peer> = match request.target {
            Some(t) => self
                .kbuckets
                .closest(&IdBytes::from(t))
                .take(usize::from(K_VALUE))
                .map(|entry| Peer::from(entry.node.value.addr))
                .collect(),
            None => {
                // TODO emit an event about this
                warn!("Got FIND_NODE without a target. Msg: {request:?}");
                return;
            }
        };
        self.io.reply(ReplyMsgData {
            tid: request.tid,
            to: peer.clone(),
            id: (!self.ephemeral).then(|| self.id.get().0),
            token: self.io.token(&peer, 1).ok(),
            closer_nodes,
            error: 0,
            value: None,
        });
    }

    /// Get the `num` closest nodes in the bucket.
    fn closer_nodes(&mut self, key: IdBytes, num: usize) -> Vec<Peer> {
        let nodes = self
            .kbuckets
            .closest(&key)
            .take(num)
            .map(|p| Peer::from(&p.node.value.addr))
            .collect::<Vec<_>>();
        nodes
        //PeersEncoding::encode(&nodes)
    }
    fn on_down_hint(&mut self, request: RequestMsgData, peer: Peer) {
        match &request.value {
            None => {
                // TODO emit an event about this
                warn!("Got DOWN_HINT with no value. Msg: {request:?}");
            }
            Some(value) => {
                if value.len() < 6 {
                    // TODO emit an event about this
                    warn!("Got DOWN_HINT with value too small. Msg: {request:?}");
                    return;
                }
                if self.down_hints_in_progress.len() < 10 {
                    let key = IdBytes::from(generic_hash(&value[..6]));
                    // NB: akwardness follows.
                    // We have to mutate some state to track the DownHint progress. but doing this
                    // is split up between outside and inside the match statement
                    //
                    // this happenes bc we take a mutable borrow of `self` entering the
                    // match statement. but in Pending/Present we want to mutate self in order
                    // track state related to DownHint. In these cases we return node.addr from the
                    // match, and use then do our self mutation.
                    //
                    // we can't return the node from the match because it's a reference.

                    // why a macro? we can't use a function because we can't take an immutable
                    // borrow of self to pass to the func
                    macro_rules! update_node_for_down_hint {
                        ($entry:tt) => {{
                            let node = $entry.value();
                            if node.pinged_within_last(Duration::from_millis(
                                crate::constants::TICK_INTERVAL_MS,
                            )) || !node.down_hint
                            {
                                node.down_hint = true;
                                node.ping_sent();
                                (node.addr, node.last_seen)
                            } else {
                                warn!(
                                    "Got DOWN_HINT for node already DOWN_HINT'd. Msg: {request:?}"
                                );
                                return;
                            }
                        }};
                    }

                    let (node_addr, last_seen) = match self.kbuckets.entry(&key) {
                        Entry::Pending(mut entry, _) => {
                            update_node_for_down_hint!(entry)
                        }
                        Entry::Present(mut entry, _) => {
                            update_node_for_down_hint!(entry)
                        }
                        _ => {
                            warn!("Got DOWN_HINT for node we don't have. Msg: {request:?}");
                            return;
                        }
                    };
                    // update self for down hint
                    let (_, tid) = self.ping(&Peer::from(node_addr));
                    self.down_hints_in_progress.push((tid, key, last_seen));
                }
                self.io.reply(ReplyMsgData {
                    tid: request.tid,
                    token: self.io.token(&peer, 1).ok(),
                    to: peer,
                    id: (!self.ephemeral).then(|| self.id.get().0),
                    closer_nodes: vec![],
                    error: 0,
                    value: None,
                })
            }
        };
    }

    fn on_ping_nat(&mut self, request: RequestMsgData, mut peer: Peer) {
        let port = match &request.value {
            Some(port_buf) => {
                if port_buf.len() < 2 {
                    // TODO emit an event about this
                    warn!("Got PING_NAT with value too small. Msg: {request:?}");
                    return;
                }
                let port = u16::from_le_bytes([port_buf[0], port_buf[1]]);
                if port == 0 {
                    warn!("Got PING_NAT with port == 0. Msg: {request:?}");
                    return;
                }
                port
            }
            None => {
                // TODO emit an event about this
                warn!("Got PING_NAT without a value. Msg: {request:?}");
                return;
            }
        };
        peer.addr.set_port(port);
        let token = self.io.token(&peer, 1).ok();
        self.io.reply(ReplyMsgData {
            tid: request.tid,
            to: peer,
            id: (!self.ephemeral).then(|| self.id.get().0),
            token,
            closer_nodes: vec![],
            error: 0,
            value: None,
        });
    }
    /// Handle a response for our Ping command
    fn on_pong(&mut self, msg: ReplyMsgData, peer: Peer) {
        // check if pong was for a downhint ping
        if let Some(pos) = self
            .down_hints_in_progress
            .iter()
            .position(|&(tid, _, _)| tid == msg.tid)
        {
            let (_, key, last_seen) = self.down_hints_in_progress.remove(pos);
            if msg.error != 0 {
                self.remove_peer(&key);
            } else {
                // we received a good response from an address than was downhinted.
                // the node either isn't down, or it is a new node (with a new id)
                // if it isn't down, node.last_seen is already updated because we have
                // already called add_node on this message. otherwise last_seen
                // is the same as it was we we pinged the node
                self.remove_stale_peer(&key, last_seen);
            }
        }

        self.queued_events
            .push_back(RpcDhtEvent::ResponseResult(Ok(ResponseOk::Pong(peer))));
    }

    /// Delegate new query event to the io handler
    fn inject_query_event(&mut self, id: QueryId, event: QueryEvent) {
        match event {
            QueryEvent::Query {
                peer,
                command,
                target,
                value,
            } => {
                self.io
                    .queue_send_request(command, Some(target.0), value, peer, Some(id), None);
            }
            QueryEvent::RemoveNode { id } => {
                self.remove_peer(&id);
            }
            QueryEvent::MissingRoundtripToken { .. } => {
                // TODO
            }
            _ => {
                todo!()
            } /*
              QueryEvent::Update {
                  peer,
                  command,
                  target,
                  value,
                  token,
              } => {
                  self.io
                      .update(command, Some(target), value, peer, token, id);
              }
              */
        }
    }

    /// Removes a peer from the routing table.
    ///
    /// Returns `None` if the peer was not in the routing table,
    /// not even pending insertion.
    pub fn remove_peer(&mut self, key: &IdBytes) -> Option<EntryView<Node>> {
        match self.kbuckets.entry(key) {
            Entry::Present(entry, _) => Some(entry.remove()),
            Entry::Pending(entry, _) => Some(entry.remove()),
            Entry::Absent(..) | Entry::SelfEntry => None,
        }
    }
    pub fn remove_stale_peer(
        &mut self,
        key: &IdBytes,
        last_seen: Instant,
    ) -> Option<EntryView<Node>> {
        match self.kbuckets.entry(key) {
            Entry::Present(mut entry, _) => {
                if entry.value().last_seen <= last_seen {
                    return Some(entry.remove());
                }
                None
            }
            Entry::Pending(mut entry, _) => {
                if entry.value().last_seen <= last_seen {
                    return Some(entry.remove());
                };
                None
            }
            Entry::Absent(..) | Entry::SelfEntry => None,
        }
    }

    /// Handles a finished query.
    fn query_finished(&mut self, query: Query) -> RpcDhtEvent {
        let is_find_node = matches!(
            query.command(),
            Command::Internal(InternalCommand::FindNode)
        );

        let result = query.into_result();

        // add nodes to the table
        for (peer, state) in result.peers {
            match state {
                PeerState::Failed => {
                    self.remove_peer(&peer.id);
                }
                PeerState::Succeeded {
                    roundtrip_token,
                    to,
                } => {
                    self.add_node(peer.id, Peer::from(peer.addr), Some(roundtrip_token), to);
                }
                _ => {}
            }
        }

        // first `find_node` query is issued as bootstrap
        if is_find_node && !self.bootstrapped {
            self.bootstrapped = true;
            RpcDhtEvent::Bootstrapped {
                stats: result.stats,
            }
        } else {
            RpcDhtEvent::QueryResult {
                id: result.inner,
                cmd: result.cmd,
                stats: result.stats,
            }
        }
    }
    /// Handles a query that timed out.
    fn query_timeout(&mut self, query: Query) -> RpcDhtEvent {
        self.query_finished(query)
    }
}
#[derive(Debug)]
pub enum RpcDhtEvent {
    /// Result wrapping an incomming Request
    /// Only emits Ok on custom commands. Errs on any request error (custom or non-custom)
    RequestResult(RequestResult),
    /// Result wrapping an incomming Response
    /// Emits for any valid response
    ResponseResult(ResponseResult),
    /// The routing table has been updated.
    RoutingUpdated {
        /// The ID of the peer that was added or updated.
        peer: Peer,
        /// The ID of the peer that was evicted from the routing table to make
        /// room for the new peer, if any.
        old_peer: Option<Peer>,
    },
    Bootstrapped {
        /// Execution statistics from the bootstrap query.
        stats: QueryStats,
    },

    /// Query arrived at final node, and neighbors need to be committed.
    // dht.request({ token: reply.token, target: query.target, command: query.command, value: query.value }, reply.from)
    ReadyToCommit {
        /// The ID of the query that finished.
        id: QueryId,
        /// The command of the executed query.
        cmd: Command,
        /// TODO store these.
        closest_replies: Vec<ReplyMsgData>,
        /// Kind of commit
        commit_kind: Commit,
    },
    /// A completed query.
    ///
    /// No more responses are expected for this query
    QueryResult {
        /// The ID of the query that finished.
        id: QueryId,
        /// The command of the executed query.
        cmd: Command,
        /// Execution statistics from the query.
        stats: QueryStats,
    },
}

pub type RequestResult = std::result::Result<RequestOk, RequestError>;

#[derive(Debug)]
pub enum RequestOk {
    /// Custom incoming request to a registered command
    ///
    /// # Note
    ///
    /// Custom commands are not automatically replied to and need to be answered
    /// manually
    CustomCommandRequest {
        /// The query we received and need to respond to
        query: CommandQuery,
        request: Box<RequestMsgData>,
        peer: Peer, // maybe peerid? or SocketAddr
    },
}

#[derive(Debug)]
pub enum RequestError {
    /// Received a query with a custom command that is not registered
    UnsupportedCommand {
        /// The unknown command
        command: String,
        /// The message we received from the peer.
        msg: RequestMsgData,
        /// The peer the message originated from.
        peer: Peer,
    },
    /// The `target` field of message was required but was empty
    MissingTarget { msg: RequestMsgData, peer: Peer },
    /// Received a message with a type other than [`Type::Query`],
    /// [`Type::Response`], [`Type::Update`]
    InvalidType {
        ty: i32,
        msg: RequestMsgData,
        peer: Peer,
    },
    /// Received a request with no command attached.
    MissingCommand { peer: Peer },
    /// Ignored Request due to message's value being this peer's id.
    InvalidValue { msg: RequestMsgData, peer: Peer },
}

pub type ResponseResult = std::result::Result<ResponseOk, ResponseError>;

#[derive(Debug)]
pub enum ResponseOk {
    /// Received a pong response to our ping request.
    Pong(Peer),
    /// A remote peer successfully responded to our query
    Response(Response),
}

#[derive(Debug)]
pub enum ResponseError {
    /// We received a bad pong to our ping request
    InvalidPong(Peer),
}

impl Stream for RpcDht {
    type Item = RpcDhtEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        let now = Instant::now();

        if let Poll::Ready(()) = pin.bootstrap_job.poll(cx, now) {
            if pin.kbuckets.iter().count() < 20 {
                pin.bootstrap();
            }
        }

        if let Poll::Ready(()) = pin.ping_job.poll(cx, now) {
            pin.ping_some();
        }
        loop {
            // Drain queued events first.
            if let Some(event) = pin.queued_events.pop_front() {
                trace!("{event:#?}");
                return Poll::Ready(Some(event));
            }

            // Look for a sent/received message
            loop {
                if let Poll::Ready(Some(event)) = Stream::poll_next(Pin::new(&mut pin.io), cx) {
                    pin.inject_event(event);
                    if let Some(event) = pin.queued_events.pop_front() {
                        trace!("{event:#?}");
                        return Poll::Ready(Some(event));
                    }
                } else {
                    match pin.queries.poll(now) {
                        QueryPoolState::Waiting(Some((query, event))) => {
                            let id = query.id();
                            pin.inject_query_event(id, event);
                        }
                        QueryPoolState::Finished(q) => {
                            let event = pin.query_finished(q);
                            trace!("{event:#?}");
                            return Poll::Ready(Some(event));
                        }
                        QueryPoolState::Timeout(q) => {
                            let event = pin.query_timeout(q);
                            trace!("{event:#?}");
                            return Poll::Ready(Some(event));
                        }
                        QueryPoolState::Waiting(None) | QueryPoolState::Idle => {
                            break;
                        }
                    }
                }
            }

            // No immediate event was produced as a result of a finished query or socket.
            // If no new events have been queued either, signal `Pending` to
            // be polled again later.
            if pin.queued_events.is_empty() {
                return Poll::Pending;
            }
        }
    }
}

// TODO just use ReplyMsgData?
/// Response received from `peer` to a request submitted by this DHT.
#[derive(Debug, Clone)]
pub struct Response {
    /// The id of the associated query
    pub query: QueryId,
    /// Command of the response message
    pub cmd: Command,
    /// `to` field of the message
    pub to: Option<SocketAddr>,
    /// Peer that issued this reponse
    pub peer: SocketAddr,
    /// Included identifier of the peer.
    pub peer_id: Option<IdBytes>,
    /// response payload
    pub value: Option<Vec<u8>>,
}

impl Response {
    /// Decodes an instance of the message from the response's value.
    pub fn decode_value<T: prost::Message + Default>(&self) -> Option<T> {
        self.value
            .as_ref()
            .and_then(|val| T::decode(val.as_slice()).ok())
    }
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Peer {
    pub id: Option<[u8; 32]>,
    pub addr: SocketAddr,
    /// Referrer that told us about this node.
    pub referrer: Option<SocketAddr>,
}

impl std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("Peer");
        if let Some(bytes) = &self.id {
            debug_struct.field("id", &format_args!("Some({})", pretty_bytes(bytes)));
        }
        if self.referrer.is_some() {
            debug_struct.field("referrer", &self.referrer);
        }
        debug_struct.field("addr", &self.addr).finish()
    }
}

impl FromStr for Peer {
    type Err = crate::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let addr: SocketAddr = s.parse()?;
        Ok(Peer {
            addr,
            id: None,
            referrer: None,
        })
    }
}

// TODO DRY
impl From<&SocketAddr> for Peer {
    fn from(value: &SocketAddr) -> Self {
        Peer {
            id: None,
            addr: *value,
            referrer: None,
        }
    }
}
impl From<&Peer> for SocketAddr {
    fn from(value: &Peer) -> Self {
        value.addr
    }
}

impl From<SocketAddr> for Peer {
    fn from(value: SocketAddr) -> Self {
        Peer {
            id: None,
            addr: value,
            referrer: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    /// Address of the peer.
    pub addr: SocketAddr,
    /// last roundtrip token in a req/resp exchanged with the peer
    pub roundtrip_token: Option<Vec<u8>>,
    /// Decoded address of the `to` message field
    pub to: Option<SocketAddr>,
    /// When a new ping is due
    pub last_seen: Instant,
    /// Known referrers available for holepunching
    pub referrers: Vec<SocketAddr>,
    /// If this node recieved a down hint
    pub down_hint: bool,
    /// Last time a ping was sent to this node
    pub last_pinged: Option<Instant>,
}

impl Node {
    fn pinged_within_last(&self, delta: Duration) -> bool {
        if let Some(x) = self.last_pinged {
            return Instant::now().duration_since(x) > delta;
        }
        true
    }

    fn ping_sent(&mut self) {
        self.last_pinged = Some(Instant::now())
    }
}

/// A 32 byte identifier for a node participating in the DHT.
#[derive(Clone, Hash, PartialOrd, PartialEq, Eq)]
pub struct IdBytes(pub [u8; PUBLIC_KEY_LENGTH]);

impl IdBytes {
    /// Create new 32 byte array with random bytes.
    pub fn random() -> Self {
        let mut key = [0u8; 32];
        fill_random_bytes(&mut key);
        Self(key)
    }

    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
    pub fn distance(&self, other: impl AsRef<[u8]>) -> Distance {
        distance(&self.0, other.as_ref())
    }
}

impl std::fmt::Debug for IdBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IdBytes({})", pretty_bytes(&self.0))
    }
}

impl Borrow<[u8]> for IdBytes {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl From<[u8; 32]> for IdBytes {
    fn from(digest: [u8; 32]) -> Self {
        Self(digest)
    }
}

impl AsRef<[u8]> for IdBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&PublicKey> for IdBytes {
    fn from(key: &PublicKey) -> Self {
        Self(key.to_bytes())
    }
}

impl TryFrom<&[u8]> for IdBytes {
    type Error = std::array::TryFromSliceError;

    fn try_from(buf: &[u8]) -> std::result::Result<Self, Self::Error> {
        Ok(Self(buf.try_into()?))
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PeerId {
    pub addr: SocketAddr,
    pub id: IdBytes,
}

impl PeerId {
    pub fn new(addr: SocketAddr, id: IdBytes) -> Self {
        Self { addr, id }
    }
}

impl Borrow<[u8]> for PeerId {
    fn borrow(&self) -> &[u8] {
        self.id.borrow()
    }
}

/// Fill the slice with random bytes
#[inline]
pub fn fill_random_bytes(dest: &mut [u8]) {
    use rand::{
        rngs::{OsRng, StdRng},
        RngCore, SeedableRng,
    };
    let mut rng = StdRng::from_rng(OsRng).unwrap();
    rng.fill_bytes(dest)
}
