use std::{num::NonZeroUsize, time::Duration};

use closest::ClosestPeersIter;
use fnv::FnvHashMap;
use futures::task::Poll;
use wasm_timer::Instant;

use crate::{
    kbucket::{distance, ALPHA_VALUE, K_VALUE},
    IdBytes, PeerId,
};

mod closest;
mod peers;
pub mod table;

use self::{
    peers::PeersIterState,
    table::{PeerState, QueryTable},
};
use super::{message::ReplyMsgData, Command, Peer, Response};

/// A `QueryPool` provides an aggregate state machine for driving `Query`s to
/// completion.
#[derive(Debug)]
pub struct QueryPool {
    local_id: IdBytes,
    queries: FnvHashMap<QueryId, Query>,
    config: QueryConfig,
    next_id: usize,
}

/// The configuration for queries in a `QueryPool`.
#[derive(Debug, Clone)]
pub struct QueryConfig {
    /// Timeout of a single query.
    pub timeout: Duration,

    /// The replication factor to use.
    pub replication_factor: NonZeroUsize,

    /// Allowed level of parallelism for iterative queries.
    pub parallelism: NonZeroUsize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        QueryConfig {
            timeout: Duration::from_secs(60),
            replication_factor: NonZeroUsize::new(K_VALUE.get()).expect("K_VALUE > 0"),
            parallelism: ALPHA_VALUE,
        }
    }
}

impl QueryPool {
    /// Creates a new `QueryPool` with the given configuration.
    pub fn new(local_id: IdBytes, config: QueryConfig) -> Self {
        Self {
            local_id,
            next_id: 0,
            config,
            queries: Default::default(),
        }
    }

    /// Returns an iterator over the queries in the pool.
    pub fn iter(&self) -> impl Iterator<Item = &Query> {
        self.queries.values()
    }

    /// Gets the current size of the pool, i.e. the number of running queries.
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }

    pub(crate) fn next_query_id(&mut self) -> QueryId {
        let id = QueryId(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    pub fn bootstrap(
        &mut self,
        target: IdBytes,
        peers: Vec<PeerId>,
        bootstrap: Vec<Peer>,
    ) -> QueryId {
        self.add_stream(
            Command::Internal(crate::InternalCommand::FindNode),
            peers,
            target,
            None,
            bootstrap,
            Commit::No,
        )
    }

    /// Adds a query to the pool.
    pub fn add_stream(
        &mut self,
        cmd: Command,
        peers: Vec<PeerId>,
        target: IdBytes,
        value: Option<Vec<u8>>,
        bootstrap: Vec<Peer>,
        commit: Commit,
    ) -> QueryId {
        let id = self.next_query_id();
        let query = Query::new(
            id,
            cmd,
            self.config.parallelism,
            self.local_id.clone(),
            target,
            value,
            peers,
            bootstrap,
            commit,
        );
        self.queries.insert(id, query);
        id
    }

    /// Returns a reference to a query with the given ID, if it is in the pool.
    pub fn get(&self, id: &QueryId) -> Option<&Query> {
        self.queries.get(id)
    }

    /// Returns a mutable reference to a query with the given ID, if it is in
    /// the pool.
    pub fn get_mut(&mut self, id: &QueryId) -> Option<&mut Query> {
        self.queries.get_mut(id)
    }

    /// Polls the pool to advance the queries.
    pub fn poll(&mut self, now: Instant) -> QueryPoolState<'_> {
        let mut finished = None;
        let mut timeout = None;
        let mut waiting = None;

        for (&query_id, query) in self.queries.iter_mut() {
            query.stats.start = query.stats.start.or(Some(now));
            match query.poll(now) {
                Poll::Ready(Some(ev)) => {
                    waiting = Some((ev, query_id));
                    break;
                }
                Poll::Ready(None) => {
                    if matches!(query.commit, Commit::No) {
                        finished = Some(query_id);
                        break;
                    }
                    return QueryPoolState::Commit(query_id);
                }
                Poll::Pending => {
                    let elapsed = now - query.stats.start.unwrap_or(now);
                    if elapsed >= self.config.timeout {
                        timeout = Some(query_id);
                        break;
                    }
                }
            }
        }

        if let Some((event, query_id)) = waiting {
            let query = self.queries.get_mut(&query_id).expect("s.a.");
            return QueryPoolState::Waiting(Some((query, event)));
        }

        if let Some(query_id) = finished {
            let mut query = self.queries.remove(&query_id).expect("s.a.");
            query.stats.end = Some(now);
            return QueryPoolState::Finished(query);
        }

        if let Some(query_id) = timeout {
            let mut query = self.queries.remove(&query_id).expect("s.a.");
            query.stats.end = Some(now);
            return QueryPoolState::Timeout(query);
        }

        if self.queries.is_empty() {
            QueryPoolState::Idle
        } else {
            QueryPoolState::Waiting(None)
        }
    }
}

/// The observable states emitted by [`QueryPool::poll`].
#[derive(Debug)]
pub enum QueryPoolState<'a> {
    /// The pool is idle, i.e. there are no queries to process.
    Idle,
    /// At least one query is waiting for results. `Some(request)` indicates
    /// that a new request is now being waited on.
    Waiting(Option<(&'a mut Query, QueryEvent)>),
    /// A query is ready to commit
    Commit(QueryId),
    /// A query has finished.
    Finished(Query),
    /// A query has timed out.
    Timeout(Query),
}

#[derive(Debug)]
pub enum Commit {
    No,
    Auto,
    Custom,
}

#[derive(Debug)]
pub struct Query {
    /// identifier for this stream
    id: QueryId,
    /// The permitted parallelism, i.e. number of pending results.
    parallelism: NonZeroUsize,
    /// The peer iterator that drives the query state.
    peer_iter: ClosestPeersIter,
    /// The rpc command of this stream
    cmd: Command,
    /// Stats about this query
    stats: QueryStats,
    /// The value to include in each message
    value: Option<Vec<u8>>,
    /// The inner query state.
    inner: QueryTable,
    /// Whether to send commits when query completes
    commit: Commit,
    /// Closest replies are store for commiting data
    closest_replies: Vec<ReplyMsgData>,
}

struct ClosestReplies {
    target: IdBytes,
    arr: Vec<ReplyMsgData>,
}

impl Query {
    pub fn new(
        id: QueryId,
        cmd: Command,
        parallelism: NonZeroUsize,
        local_id: IdBytes,
        target: IdBytes,
        value: Option<Vec<u8>>,
        peers: Vec<PeerId>,
        bootstrap: Vec<Peer>,
        commit: Commit,
    ) -> Self {
        Self {
            id,
            parallelism,
            peer_iter: ClosestPeersIter::new(target.clone().into(), bootstrap),
            cmd,
            stats: QueryStats::empty(),
            value,
            inner: QueryTable::new(local_id, target, peers),
            commit,
            closest_replies: vec![],
        }
    }

    // TODO in theory, new elements distances get smaller. So maybe reverse the list.
    // if that does not really hold true use a binary search
    fn maybe_insert(&mut self, reply: ReplyMsgData) -> Option<usize> {
        let reply_distance = self.peer_iter.target.distance(reply.id?);
        let replace = self.closest_replies.len() >= K_VALUE.into();

        for (i, cur) in self.closest_replies.iter().enumerate() {
            if reply_distance
                < self
                    .peer_iter
                    .target
                    .distance(cur.id.expect("TODO this should be a PeerId"))
            {
                if replace {
                    self.closest_replies[i] = reply;
                } else {
                    self.closest_replies.insert(i, reply.clone());
                }
                return Some(i);
            }
        }
        None
    }
    pub fn command(&self) -> &Command {
        &self.cmd
    }

    pub fn target(&self) -> &IdBytes {
        self.inner.target()
    }
    pub fn value(&self) -> Option<&Vec<u8>> {
        self.value.as_ref()
    }

    pub fn id(&self) -> QueryId {
        self.id
    }

    pub(crate) fn on_timeout(&mut self, peer: Peer) {
        self.stats.failure += 1;
        for (p, state) in self.inner.peers_mut() {
            if p.addr == peer.addr {
                *state = PeerState::Failed;
                break;
            }
        }
        self.peer_iter.on_failure(&peer);
    }

    /// Received a response to a requested driven by this query.
    pub(crate) fn inject_response(
        &mut self,
        mut resp: ReplyMsgData,
        peer: Peer,
    ) -> Option<Response> {
        self.maybe_insert(resp.clone());
        let remote = resp.id.map(|id| PeerId::new(peer.addr, IdBytes::from(id)));

        if resp.is_error() {
            self.stats.failure += 1;
            self.peer_iter.on_failure(&peer);
            if let Some(ref remote) = remote {
                if let Some(state) = self.inner.peers_mut().get_mut(remote) {
                    *state = PeerState::Failed;
                }
            }
            return None;
        }

        self.stats.success += 1;
        self.peer_iter.on_success(&peer, &resp.closer_nodes);

        if let Some(token) = resp.token.take() {
            if let Some(remote) = remote {
                self.inner
                    .add_verified(remote, token.to_vec(), Some(resp.to.addr));
            }
        }

        Some(Response {
            query: self.id,
            cmd: self.cmd,
            to: Some(resp.to.addr),
            peer: peer.addr,
            peer_id: resp.valid_id_bytes(),
            value: resp.value,
        })
    }

    fn send(&mut self, peer: Peer, update: bool) -> QueryEvent {
        if update {
            if let Some(token) = self.inner.get_token(&peer) {
                QueryEvent::Update {
                    command: self.cmd,
                    token: Some(token.clone()),
                    target: self.target().clone(),
                    peer,
                    value: self.value.clone(),
                }
            } else {
                // don't wait for a response
                self.peer_iter.on_failure(&peer);
                QueryEvent::MissingRoundtripToken { peer }
            }
        } else {
            QueryEvent::Query {
                command: self.cmd,
                target: self.target().clone(),
                value: self.value.clone(),
                peer,
            }
        }
    }

    fn poll(&mut self, now: Instant) -> Poll<Option<QueryEvent>> {
        match self.peer_iter.next(now) {
            PeersIterState::Waiting(peer) => {
                if let Some(peer) = peer {
                    Poll::Ready(Some(self.send(peer, false)))
                } else {
                    Poll::Pending
                }
            }
            PeersIterState::WaitingAtCapacity => Poll::Pending,
            PeersIterState::Finished => Poll::Ready(None),
        }
    }

    /// Consumes the query, producing the final `QueryResult`.
    pub fn into_result(self) -> QueryResult<QueryId, impl Iterator<Item = (PeerId, PeerState)>> {
        QueryResult {
            peers: self.inner.into_result(),
            inner: self.id,
            stats: self.stats,
            cmd: self.cmd,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum QueryType {
    Query,
    // TODO this can be removed
    Update,
    // TODO this can be removed
    QueryUpdate,
}

impl QueryType {
    pub fn is_query(&self) -> bool {
        matches!(self, QueryType::Query | QueryType::QueryUpdate)
    }

    // TODO update type is removed
    pub fn is_update(&self) -> bool {
        matches!(self, QueryType::Update | QueryType::QueryUpdate)
    }
}

#[derive(Debug)]
pub enum QueryEvent {
    Query {
        peer: Peer,
        command: Command,
        target: IdBytes,
        value: Option<Vec<u8>>,
    },
    RemoveNode {
        id: IdBytes,
    },
    MissingRoundtripToken {
        peer: Peer,
    },
    // TODO update type is removed
    Update {
        peer: Peer,
        command: Command,
        target: IdBytes,
        value: Option<Vec<u8>>,
        token: Option<Vec<u8>>,
    },
}

/// Represents an incoming query with a custom command.
#[derive(Debug, Clone)]
pub struct CommandQuery {
    /// The Id of the query
    pub tid: u16,
    /// Command def
    pub command: usize,
    /// the node who sent the query/update
    pub peer: Peer,
    /// the query/update target (32 byte target)
    pub target: IdBytes,
    /// the query/update payload decoded with the inputEncoding
    pub value: Option<Vec<u8>>,
}

impl CommandQuery {
    pub fn into_response_with_error(self, err: impl Into<usize>) -> CommandQueryResponse {
        let mut resp = CommandQueryResponse::from(self);
        resp.msg.error = err.into();
        resp
    }
}

/// Outgoing response to a `CommandQuery`
#[derive(Debug, Clone)]
pub struct CommandQueryResponse {
    pub msg: ReplyMsgData,
    pub peer: Peer,
    pub target: IdBytes,
}

impl From<CommandQuery> for CommandQueryResponse {
    fn from(q: CommandQuery) -> Self {
        let msg = ReplyMsgData {
            tid: q.tid,
            to: q.peer.clone(),
            id: None,
            token: None,
            closer_nodes: vec![],
            value: q.value,
            error: 0,
        };

        Self {
            msg,
            peer: q.peer,
            target: q.target,
        }
    }
}
/// The result of a `Query`.
pub struct QueryResult<TInner, TPeers> {
    /// The opaque inner query state.
    pub inner: TInner,
    /// The successfully contacted peers.
    pub peers: TPeers,
    /// The collected query statistics.
    pub stats: QueryStats,
    /// The Command of the query.
    pub cmd: Command,
}

/// Execution statistics of a query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryStats {
    requests: u32,
    success: u32,
    failure: u32,
    start: Option<Instant>,
    end: Option<Instant>,
}

impl QueryStats {
    pub fn empty() -> Self {
        QueryStats {
            requests: 0,
            success: 0,
            failure: 0,
            start: None,
            end: None,
        }
    }

    /// Gets the total number of requests initiated by the query.
    pub fn num_requests(&self) -> u32 {
        self.requests
    }

    /// Gets the number of successful requests.
    pub fn num_successes(&self) -> u32 {
        self.success
    }

    /// Gets the number of failed requests.
    pub fn num_failures(&self) -> u32 {
        self.failure
    }

    /// Gets the number of pending requests.
    ///
    /// > **Note**: A query can finish while still having pending
    /// > requests, if the termination conditions are already met.
    pub fn num_pending(&self) -> u32 {
        self.requests - (self.success + self.failure)
    }

    /// Gets the duration of the query.
    ///
    /// If the query has not yet finished, the duration is measured from the
    /// start of the query to the current instant.
    ///
    /// If the query did not yet start (i.e. yield the first peer to contact),
    /// `None` is returned.
    pub fn duration(&self) -> Option<Duration> {
        if let Some(s) = self.start {
            if let Some(e) = self.end {
                Some(e - s)
            } else {
                Some(Instant::now() - s)
            }
        } else {
            None
        }
    }

    /// Merges these stats with the given stats of another query,
    /// e.g. to accumulate statistics from a multi-phase query.
    ///
    /// Counters are merged cumulatively while the instants for
    /// start and end of the queries are taken as the minimum and
    /// maximum, respectively.
    pub fn merge(self, other: QueryStats) -> Self {
        QueryStats {
            requests: self.requests + other.requests,
            success: self.success + other.success,
            failure: self.failure + other.failure,
            start: match (self.start, other.start) {
                (Some(a), Some(b)) => Some(std::cmp::min(a, b)),
                (a, b) => a.or(b),
            },
            end: std::cmp::max(self.end, other.end),
        }
    }
}
/// Unique identifier for an active query.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct QueryId(pub usize);
