use crate::IdBytes;

use super::{smod::Peer, Command};

#[derive(Debug, Clone, PartialEq)]
pub struct ReplyMsgData {
    pub tid: u16,
    pub to: Peer,
    pub id: Option<[u8; 32]>,
    pub token: Option<[u8; 32]>,
    pub closer_nodes: Vec<Peer>,
    pub error: usize,
    pub value: Option<Vec<u8>>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct RequestMsgData {
    pub tid: u16,
    pub to: Peer,
    pub id: Option<[u8; 32]>,
    pub internal: bool,
    pub token: Option<[u8; 32]>,
    pub command: Command,
    pub target: Option<[u8; 32]>,
    pub value: Option<Vec<u8>>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum MsgData {
    Request(RequestMsgData),
    Reply(ReplyMsgData),
}

impl From<RequestMsgData> for MsgData {
    fn from(value: RequestMsgData) -> Self {
        MsgData::Request(value)
    }
}
impl From<ReplyMsgData> for MsgData {
    fn from(value: ReplyMsgData) -> Self {
        MsgData::Reply(value)
    }
}

pub fn valid_id_bytes(id: Option<[u8; 32]>) -> Option<IdBytes> {
    id.map(IdBytes::from)
}

impl MsgData {
    pub fn to(&self) -> Peer {
        match self {
            MsgData::Request(x) => x.to.clone(),
            MsgData::Reply(x) => x.to.clone(),
        }
    }

    pub(crate) fn valid_id_bytes(&self) -> Option<IdBytes> {
        let id = match self {
            MsgData::Request(r) => r.id,
            MsgData::Reply(r) => r.id,
        };
        valid_id_bytes(id)
    }
}
