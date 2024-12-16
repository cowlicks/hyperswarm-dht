use crate::{
    util::{debug_vec, pretty_bytes},
    IdBytes,
};

use super::{Command, Peer};

#[derive(Clone, PartialEq)]
pub struct ReplyMsgData {
    pub tid: u16,
    pub to: Peer,
    pub id: Option<[u8; 32]>,
    /// TODO remove option? we always send token with reply I thought
    pub token: Option<[u8; 32]>,
    pub closer_nodes: Vec<Peer>,
    pub error: usize,
    pub value: Option<Vec<u8>>,
}
#[derive(Clone, PartialEq)]
pub struct RequestMsgData {
    pub tid: u16,
    pub to: Peer,
    pub id: Option<[u8; 32]>,
    // TODO rm this, it is encoded in command
    pub internal: bool,
    pub token: Option<[u8; 32]>,
    pub command: Command,
    pub target: Option<[u8; 32]>,
    pub value: Option<Vec<u8>>,
}

macro_rules! opt_map_inner {
    ($debug_struct:tt, $name:expr, $name_s:tt, $func:tt) => {
        match &$name {
            Some(bytes) => $debug_struct.field($name_s, &format_args!("Some({})", $func(bytes))),
            None => $debug_struct.field("id", &None::<String>),
        };
    };
}

impl std::fmt::Debug for RequestMsgData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("RequestMsgData");
        debug_struct.field("tid", &self.tid).field("to", &self.to);
        opt_map_inner!(debug_struct, self.id, "id", pretty_bytes);
        debug_struct.field("internal", &self.internal);
        opt_map_inner!(debug_struct, self.token, "token", pretty_bytes);
        debug_struct.field("command", &self.command);
        opt_map_inner!(debug_struct, self.target, "target", pretty_bytes);
        opt_map_inner!(debug_struct, self.value, "value", pretty_bytes);
        debug_struct.finish()
    }
}
impl std::fmt::Debug for ReplyMsgData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("ReplyMsgData");
        debug_struct.field("tid", &self.tid).field("to", &self.to);
        opt_map_inner!(debug_struct, self.id, "id", pretty_bytes);
        opt_map_inner!(debug_struct, self.token, "token", pretty_bytes);
        debug_struct
            .field("closer_nodes", &debug_vec(&self.closer_nodes))
            .field("error", &self.error);
        opt_map_inner!(debug_struct, self.value, "value", pretty_bytes);
        debug_struct.finish()
    }
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
