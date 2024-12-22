#![allow(unused)]
use futures::channel::mpsc::{self, Receiver, Sender};
/// Commit
/// * commit request - An outgoing [`ReplyMsgData`] which includes a valid token. When the message
/// is recieved and verified, the reciever does some mutation of state
use std::{
    collections::{BTreeMap, BTreeSet},
    iter::FromIterator,
    net::SocketAddr,
};

use crate::{constants::DEFAULT_COMMIT_CHANNEL_SIZE, io::Tid};

use crate::{query::QueryId, Command, IdBytes};

#[derive(Debug)]
pub enum CommitMessage {
    Send(CommitRequestParams),
    Done,
}

#[derive(Debug)]
pub struct CommitRequestParams {
    command: Command,
    target: Option<IdBytes>,
    value: Option<Vec<u8>>,
    peer: SocketAddr,
    query_id: QueryId,
    token: [u8; 32],
}

struct Commiter {
    rx: CommitMessage,
}

/// Events to send to [`RpcDht`] for handling
enum Event {
    SendCommitRequest(CommitRequestParams),
    CommitDone,
}

#[derive(Debug)]
pub enum Commit {
    No,
    Auto(Progress),
    //Custom((Option<Receiver<CommitMessages>>, Progress)),
    //Custom(Custom),
    Custom(Progress),
}

#[derive(Debug)]
pub enum Progress {
    BeforeStart,
    /// Sending commit requests in progress. tids are added as they are sent, but also can be
    /// removed when responses are recieved
    Sending((Receiver<CommitMessage>, BTreeSet<Tid>)),
    /// awaiting replies to commit messages
    AwaitingReplies(BTreeSet<Tid>),
    Done,
}

use Progress::*;
impl Progress {
    pub fn start_sending(&mut self) -> Sender<CommitMessage> {
        if matches!(self, BeforeStart) {
            let (tx, rx) = mpsc::channel(DEFAULT_COMMIT_CHANNEL_SIZE);
            *self = Sending((rx, Default::default()));
            tx
        } else {
            panic!("Tried to start sending but already started");
        }
    }

    pub fn transition_to_awaiting(&mut self) {
        *self = AwaitingReplies(match self {
            Sending((_, tids)) => tids.clone(),
            _ => panic!("not in sending"),
        })
    }

    pub fn poll(&mut self) -> Option<CommitMessage> {
        let Sending((rx, _tids)) = self else {
            panic!("poll while not sending");
        };
        rx.try_next().ok().flatten()
    }
    pub fn sent_tid(&mut self) {
        // insert to Sending.tids
    }

    pub fn start_awaiting(&mut self, tids: Vec<Tid>) {
        if matches!(self, BeforeStart | Sending(_)) {
            *self = AwaitingReplies(BTreeSet::from_iter(tids))
        } else {
            panic!("Tried to start commit that was already started");
        }
    }
    pub fn recieved_tid(&mut self, tid: Tid) -> bool {
        let done = match self {
            Self::AwaitingReplies(tids) => {
                tids.remove(&tid);
                tids.is_empty()
            }
            Self::Sending((_, tids)) => {
                tids.remove(&tid);
                false
            }
            _ => false,
        };
        if done {
            *self = Done;
        }
        matches!(self, Done)
    }
}
