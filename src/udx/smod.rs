//! Make RPC calls over a Kademlia based DHT.

use std::{
    collections::{HashSet, VecDeque},
    net::{SocketAddr, ToSocketAddrs},
    time::Duration,
};

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
    mslave::Master,
    query::{table::Peer, CommandQuery, QueryConfig, QueryPool, QueryStats, QueryType},
    sio::{IoConfig, IoHandler},
    stream::MessageDataStream,
    thirty_two_random_bytes, Addr, Command, InternalCommand,
};

#[derive(Debug, derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct RpcDht {
    // TODO use message passing to update id's in IoHandler
    #[builder(default = "Master::new(Key::new(IdBytes::from(thirty_two_random_bytes())))")]
    pub id: Master<Key<IdBytes>>,
    kbuckets: KBucketsTable<Key<IdBytes>, Addr>,
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
            .map(|e| {
                let sa = SocketAddr::from((e.node.value.host, e.node.value.port));
                PeerId::new(sa, e.node.key.preimage().clone())
            })
            .map(Key::new)
            .collect::<Vec<_>>();

        /*
        let nodes: Vec<Peer> = self
            .bootstrap_nodes
            .clone()
            .into_iter()
            .map(Peer::from)
            .collect();
        */
        self.queries.add_stream(
            cmd,
            peers,
            todo!(),
            target,
            value,
            //
            //nodes)
            todo!(),
        )
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
