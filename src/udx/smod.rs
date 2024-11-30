//! Make RPC calls over a Kademlia based DHT.

use std::{
    collections::{HashSet, VecDeque},
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
    time::Duration,
};
use tracing::debug;
use wasm_timer::Instant;

use futures::Stream;

use crate::{
    kbucket::{
        Entry,
        EntryView,
        InsertResult,
        KBucketsTable,
        Key,
        KeyBytes,
        NodeStatus,
        K_VALUE, //KeyBytes,
    },
    peers::PeersEncoding,
    rpc::{jobs::PeriodicJob, query::QueryId},
    util::pretty_bytes,
    IdBytes, PeerId,
};

use super::{
    message::{valid_id_bytes, ReplyMsgData, RequestMsgData},
    mslave::Master,
    query::{
        table::PeerState, CommandQuery, QueryConfig, QueryEvent, QueryPool, QueryPoolState,
        QueryStats, QueryStream,
    },
    sio::{IoConfig, IoHandler, IoHandlerEvent},
    stream::MessageDataStream,
    thirty_two_random_bytes, Command, InternalCommand,
};

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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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

#[derive(Debug, derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct RpcDht {
    // TODO use message passing to update id's in IoHandler
    #[builder(default = "Master::new(Key::new(IdBytes::from(thirty_two_random_bytes())))")]
    pub id: Master<Key<IdBytes>>,
    kbuckets: KBucketsTable<Key<IdBytes>, Node>,
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
}

#[derive(Debug)]
pub struct DhtConfig {
    kbucket_pending_timeout: Duration,
    local_id: Option<[u8; 32]>,
    commands: HashSet<usize>,
    query_config: QueryConfig,
    io_config: IoConfig,
    bootstrap_interval: Duration,
    ping_interval: Duration,
    #[allow(unused)] // FIXME bg: why never read?
    connection_idle_timeout: Duration,
    ephemeral: bool,
    pub(crate) adaptive: bool,
    bootstrap_nodes: Vec<SocketAddr>,
    socket: Option<MessageDataStream>,
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
}

impl RpcDht {
    pub async fn with_config(config: DhtConfig) -> crate::Result<Self> {
        let bites = config.local_id.unwrap_or_else(thirty_two_random_bytes);
        let id_bytes = IdBytes::from(bites);
        let local_id = Key::new(id_bytes.clone());
        let id = Master::new(local_id.clone());

        let socket = config
            .socket
            .map(Result::Ok)
            .unwrap_or_else(MessageDataStream::defualt_bind)?;

        let io = IoHandler::new(id.view(), socket, config.io_config);

        let mut dht = Self {
            id,
            kbuckets: KBucketsTable::new(local_id.clone(), config.kbucket_pending_timeout),
            io,
            bootstrap_job: PeriodicJob::new(config.bootstrap_interval),
            ping_job: PeriodicJob::new(config.ping_interval),
            queries: QueryPool::new(local_id, config.query_config),
            commands: config.commands,
            queued_events: Default::default(),
            bootstrap_nodes: config.bootstrap_nodes,
            bootstrapped: false,
        };

        dht.bootstrap();
        Ok(dht)
    }

    pub fn bootstrap(&mut self) {
        if !self.bootstrap_nodes.is_empty() {
            self.query(
                InternalCommand::FindNode.into(),
                self.id.get().clone(),
                None,
            );
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

    pub fn query(&mut self, cmd: Command, target: Key<IdBytes>, value: Option<Vec<u8>>) -> QueryId {
        self.run_command(cmd, target, value)
    }

    fn run_command(
        &mut self,
        cmd: Command,
        target: Key<IdBytes>,
        value: Option<Vec<u8>>,
    ) -> QueryId {
        let peers = self
            .kbuckets
            .closest(&target)
            .take(usize::from(K_VALUE))
            .map(|e| PeerId::new(e.node.value.addr, e.node.key.preimage().clone()))
            .map(Key::new)
            .collect::<Vec<_>>();

        let bootstrap_nodes: Vec<Peer> = self.bootstrap_nodes.iter().map(Peer::from).collect();
        self.queries
            .add_stream(cmd, peers, target, value, bootstrap_nodes)
    }

    fn ping_some(&mut self) {
        let cnt = if self.queries.len() > 2 { 3 } else { 5 };
        let now = Instant::now();
        for peer in self
            .kbuckets
            .iter()
            .filter_map(|entry| {
                if now > entry.node.value.next_ping {
                    Some(PeerId::new(
                        entry.node.value.addr,
                        entry.node.key.preimage().clone(),
                    ))
                } else {
                    None
                }
            })
            .take(cnt)
            .collect::<Vec<_>>()
        {
            self.ping(&peer)
        }
    }

    /// Ping a remote
    pub fn ping(&mut self, peer: &PeerId) {
        self.io.query(
            Command::Internal(InternalCommand::Ping),
            None,
            Some(peer.id.clone().to_vec()),
            (&peer.addr).into(),
            self.queries.next_query_id(),
        )
    }

    /// Handle the event generated from the underlying IO
    fn inject_event(&mut self, event: IoHandlerEvent) {
        match event {
            IoHandlerEvent::OutResponse { .. } => {}
            IoHandlerEvent::OutSocketErr { .. } => {}
            IoHandlerEvent::InRequest { message, peer } => {
                self.on_request(message, peer.into());
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
                //if let Some(query) = self.queries.get_mut(&user_data) {
                //    query.on_timeout(peer);
                //}
            }
        }
    }

    /// Process a response.
    fn on_response(
        &mut self,
        req: Box<RequestMsgData>,
        resp: ReplyMsgData,
        peer: Peer,
        query_id: QueryId,
    ) {
        if matches!(req.command, Command::Internal(InternalCommand::Ping)) {
            self.on_pong(resp, peer);
            return;
        }

        if let Some(query) = self.queries.get_mut(&query_id) {
            if let Some(resp) = query.inject_response(resp, peer.into()) {
                self.queued_events
                    .push_back(RpcDhtEvent::ResponseResult(Ok(ResponseOk::Response(resp))))
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
                InternalCommand::Ping => self.on_ping(msg, peer),
                InternalCommand::FindNode => self.on_findnode(msg, peer),
                InternalCommand::PingNat => self.on_ping_nat(msg, peer),
                //Unknown(s) => self.on_command_req(ty, s, msg, peer),
                _ => todo!(),
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
        let key = Key::new(id);
        match self.kbuckets.entry(&key) {
            Entry::Present(mut entry, _) => {
                entry.value().next_ping = Instant::now() + self.ping_job.interval;
            }
            Entry::Pending(mut entry, _) => {
                let n = entry.value();
                n.addr = peer.addr;
                n.next_ping = Instant::now() + self.ping_job.interval;
            }
            Entry::Absent(entry) => {
                let node = Node {
                    addr: peer.addr,
                    roundtrip_token,
                    to,
                    next_ping: Instant::now() + self.ping_job.interval,
                    referrers: vec![],
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
    /// Handle a ping request
    fn on_ping(&mut self, msg: RequestMsgData, peer: Peer) {
        if let Some(ref val) = msg.value {
            if self.id.get().preimage() != val {
                // ping wasn't meant for this node
                self.queued_events.push_back(RpcDhtEvent::RequestResult(Err(
                    RequestError::InvalidValue { peer, msg },
                )));
                return;
            }
        }
        // TODO handle result
        let _todo = self
            .io
            .response(msg, Some(PeersEncoding::encode(&peer.addr)), None, peer);
    }
    /// Handle an incoming find peers request.
    ///
    /// Reply only if the remote provided a target to get the closest nodes for.
    fn on_findnode(&mut self, msg: RequestMsgData, peer: Peer) {
        if let Some(key) = valid_id_bytes(msg.id) {
            let closer_nodes = self.closer_nodes(key, usize::from(K_VALUE));
            // TODO result
            let _ = self.io.response(msg, None, Some(closer_nodes), peer);
        }
    }

    /// Get the `num` closest nodes in the bucket.
    fn closer_nodes(&mut self, key: IdBytes, num: usize) -> Vec<Peer> {
        let nodes = self
            .kbuckets
            .closest(&KeyBytes::new(key))
            .take(num)
            .map(|p| Peer::from(&p.node.value.addr))
            .collect::<Vec<_>>();
        nodes
        //PeersEncoding::encode(&nodes)
    }
    fn on_ping_nat(&self, _msg: RequestMsgData, _peer: Peer) {
        todo!()
    }
    /// Handle a response for our Ping command
    fn on_pong(&mut self, msg: ReplyMsgData, peer: Peer) {
        if let Some(id) = msg.valid_id_bytes() {
            match self.kbuckets.entry(&Key::new(id)) {
                Entry::Present(mut entry, _) => {
                    entry.value().next_ping = Instant::now() + self.ping_job.interval;
                    self.queued_events.push_back(RpcDhtEvent::ResponseResult(Ok(
                        ResponseOk::Pong(Peer::from(&entry.value().addr)),
                    )));
                    return;
                }
                Entry::Pending(mut entry, _) => {
                    entry.value().next_ping = Instant::now() + self.ping_job.interval;
                    self.queued_events.push_back(RpcDhtEvent::ResponseResult(Ok(
                        ResponseOk::Pong(Peer::from(&entry.value().addr)),
                    )));
                    return;
                }
                _ => {}
            }
        }

        self.queued_events
            .push_back(RpcDhtEvent::ResponseResult(Err(
                ResponseError::InvalidPong(peer),
            )))
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
                self.io.query(command, Some(target.0), value, peer, id);
            }
            QueryEvent::RemoveNode { id } => {
                self.remove_peer(&Key::new(id));
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
    pub fn remove_peer(&mut self, key: &Key<IdBytes>) -> Option<EntryView<Key<IdBytes>, Node>> {
        match self.kbuckets.entry(key) {
            Entry::Present(entry, _) => Some(entry.remove()),
            Entry::Pending(entry, _) => Some(entry.remove()),
            Entry::Absent(..) | Entry::SelfEntry => None,
        }
    }

    /// Handles a finished query.
    fn query_finished(&mut self, query: QueryStream) -> RpcDhtEvent {
        let is_find_node = matches!(
            query.command(),
            Command::Internal(InternalCommand::FindNode)
        );

        let result = query.into_result();

        // add nodes to the table
        for (peer, state) in result.peers {
            match state {
                PeerState::Failed => {
                    self.remove_peer(&Key::new(peer.id));
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
    fn query_timeout(&mut self, query: QueryStream) -> RpcDhtEvent {
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

pub type RequestResult = Result<RequestOk, RequestError>;

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

pub type ResponseResult = Result<ResponseOk, ResponseError>;

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

#[derive(Debug, Clone)]
pub struct Node {
    /// Address of the peer.
    pub addr: SocketAddr,
    /// last roundtrip token in a req/resp exchanged with the peer
    pub roundtrip_token: Option<Vec<u8>>,
    /// Decoded address of the `to` message field
    pub to: Option<SocketAddr>,
    /// When a new ping is due
    pub next_ping: Instant,
    /// Known referrers available for holepunching
    pub referrers: Vec<SocketAddr>,
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
            pin.ping_some()
        }
        loop {
            // Drain queued events first.
            if let Some(event) = pin.queued_events.pop_front() {
                return Poll::Ready(Some(event));
            }

            // Look for a sent/received message
            loop {
                if let Poll::Ready(Some(event)) = Stream::poll_next(Pin::new(&mut pin.io), cx) {
                    pin.inject_event(event);
                    if let Some(event) = pin.queued_events.pop_front() {
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
                            return Poll::Ready(Some(event));
                        }
                        QueryPoolState::Timeout(q) => {
                            let event = pin.query_timeout(q);
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
