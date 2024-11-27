//! Make RPC calls over a Kademlia based DHT.

use std::{
    collections::{HashSet, VecDeque},
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use wasm_timer::Instant;

use futures::Stream;

use crate::{
    kbucket::{
        KBucketsTable,
        Key,
        K_VALUE, //KeyBytes,
    },
    rpc::{jobs::PeriodicJob, query::QueryId},
    IdBytes, PeerId,
};

use super::{
    cenc::{
        // MsgData,
        ReplyMsgData,
        RequestMsgData,
    },
    message::valid_id_bytes,
    mslave::Master,
    query::{CommandQuery, QueryConfig, QueryPool, QueryStats},
    sio::{IoConfig, IoHandler, IoHandlerEvent},
    stream::MessageDataStream,
    thirty_two_random_bytes, Addr, Command, InternalCommand,
};

#[derive(Clone, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct Peer {
    pub addr: SocketAddr,
    /// Referrer that told us about this node.
    pub referrer: Option<SocketAddr>,
}

impl From<Addr> for Peer {
    fn from(value: Addr) -> Self {
        Self {
            addr: SocketAddr::from(&value),
            referrer: None,
        }
    }
}

impl From<&SocketAddr> for Peer {
    fn from(value: &SocketAddr) -> Self {
        Peer {
            addr: *value,
            referrer: None,
        }
    }
}

impl From<SocketAddr> for Peer {
    fn from(value: SocketAddr) -> Self {
        Peer {
            addr: value.clone(),
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
            IoHandlerEvent::InResponseBadRequestId { peer, .. } => {
                // received a response that did not match any issued requests
                //self.remove_node(&peer);
                todo!()
            }
            IoHandlerEvent::OutRequest { .. } => {
                // sent a request
            }
            IoHandlerEvent::InResponse { req, resp, peer } => {
                //self.on_response(req, resp, peer, user_data);
                todo!()
            }
            IoHandlerEvent::RequestTimeout {
                message,
                peer,
                sent: _,
            } => {
                todo!()
                //if let Some(query) = self.queries.get_mut(&user_data) {
                //    query.on_timeout(peer);
                //}
            }
        }
    }

    /// Handle an incoming request.
    ///
    /// Eventually send a response.
    fn on_request(&mut self, mut msg: RequestMsgData, peer: Peer) {
        if let Some(id) = valid_id_bytes(msg.id) {
            //self.add_node(id, peer.clone(), None, msg.to);
        }

        /*
        if let Some(cmd) = msg.get_command() {
            match cmd {
                Command::Ping => self.on_ping(msg, peer),
                Command::FindNode => self.on_findnode(msg, peer),
                Command::PingNat => self.on_ping_nat(msg, peer),
                Command::Unknown(s) => self.on_command_req(ty, s, msg, peer),
            };
        } else {
            // TODO refactor with oncommand fn
            if msg.target.is_none() {
                self.queued_events.push_back(RpcDhtEvent::RequestResult(Err(
                    RequestError::MissingTarget { peer, msg },
                )));
                return;
            }
            if let Some(key) = msg.valid_target_id_bytes() {
                msg.error = Some("Unsupported command".to_string());
                self.reply(msg, peer.clone(), key);
            }
            self.queued_events.push_back(RpcDhtEvent::RequestResult(Err(
                RequestError::MissingCommand { peer },
            )));
        }
            */
    }

    fn add_node(
        &mut self,
        id: IdBytes,
        peer: Peer,
        roundtrip_token: Option<Vec<u8>>,
        to: Option<SocketAddr>,
    ) {
        let key = Key::new(id);
        /*
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

                match entry.insert(node, NodeStatus::Connected) {
                    kbucket::InsertResult::Inserted => {
                        self.queued_events.push_back(RpcDhtEvent::RoutingUpdated {
                            peer,
                            old_peer: None,
                        });
                    }
                    kbucket::InsertResult::Full => {
                        log::debug!("Bucket full. Peer not added to routing table: {:?}", peer)
                    }
                    kbucket::InsertResult::Pending { disconnected: _ } => {

                        // TODO dial remote
                    }
                }
            }
            Entry::SelfEntry => {}
        }
        */
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
        peer: Addr,
        /// The ID of the peer that was evicted from the routing table to make
        /// room for the new peer, if any.
        old_peer: Option<Addr>,
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
        cmd: InternalCommand,
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
        peer: Addr,
    },
    /// The `target` field of message was required but was empty
    MissingTarget { msg: RequestMsgData, peer: Addr },
    /// Received a message with a type other than [`Type::Query`],
    /// [`Type::Response`], [`Type::Update`]
    InvalidType {
        ty: i32,
        msg: RequestMsgData,
        peer: Addr,
    },
    /// Received a request with no command attached.
    MissingCommand { peer: Addr },
    /// Ignored Request due to message's value being this peer's id.
    InvalidValue { msg: RequestMsgData, peer: Addr },
}

pub type ResponseResult = Result<ResponseOk, ResponseError>;

#[derive(Debug)]
pub enum ResponseOk {
    /// Received a pong response to our ping request.
    Pong(Addr),
    /// A remote peer successfully responded to our query
    Response(ReplyMsgData),
}

#[derive(Debug)]
pub enum ResponseError {
    /// We received a bad pong to our ping request
    InvalidPong(Addr),
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
                    /*
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
                    */
                    todo!()
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
