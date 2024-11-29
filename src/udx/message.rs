use crate::IdBytes;

use super::{
    cenc::{MsgData, ReplyMsgData, RequestMsgData},
    smod::Peer,
};

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
