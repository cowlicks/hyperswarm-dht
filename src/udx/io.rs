#[allow(unreachable_code, dead_code)]
use std::sync::{Arc, RwLock};
use std::{collections::BTreeMap, net::SocketAddr};
use tracing::warn;

use tokio::sync::oneshot::Sender;

use crate::{
    constants::{REQUEST_ID, RESPONSE_ID},
    Result,
};
use compact_encoding::State;

use super::{
    cenc::{generic_hash, generic_hash_with_key, ipv4, validate_id},
    thirty_two_random_bytes, Command, Peer,
};

use crate::kbucket::Key;
use crate::rpc::query::QueryId;
use crate::{udx, IdBytes};
use fnv::FnvHashMap;
use futures::Stream;
use futures::{
    task::{Context, Poll},
    Sink,
};
use rand::Rng;
use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::atomic::{AtomicU16, Ordering},
    time::Duration,
};
use tracing::trace;
use wasm_timer::Instant;

use super::message::{MsgData, ReplyMsgData, RequestMsgData};
use super::mslave::Slave;
use super::stream::MessageDataStream;
use super::QueryAndTid;

pub const VERSION: u64 = 1;

const ROTATE_INTERVAL: u64 = 300_000;

type Tid = u16;

#[derive(Debug, PartialEq, Clone)]
pub struct Reply {
    pub tid: u16,
    pub rtt: usize,
    // who sent reply
    pub from: Peer,
    // who recieved it
    pub to: Peer,
    pub token: Option<[u8; 32]>,
    pub closer_nodes: Vec<Peer>,
    pub error: usize,
    pub value: Option<Vec<u8>>,
}

impl Reply {
    pub fn decode(from: &Peer, buff: &[u8], state: &mut State) -> Result<Self> {
        let data = ReplyMsgData::decode(buff, state)?;
        Self::from_data(from, data)
    }
    fn from_data(from: &Peer, data: ReplyMsgData) -> Result<Self> {
        let mut new_from = from.clone();
        if let Some(id) = data.id {
            new_from.id = validate_id(&id, from);
        }
        Ok(Reply {
            tid: data.tid,
            rtt: 0,
            from: new_from,
            to: data.to,
            token: data.token,
            closer_nodes: data.closer_nodes,
            error: data.error,
            value: data.value,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub tid: u16,
    pub from: Option<Peer>,
    // TODO remove this field?
    pub to: Peer,
    pub token: Option<[u8; 32]>,
    pub internal: bool,
    pub command: Command,
    pub target: Option<[u8; 32]>,
    pub value: Option<Vec<u8>>,
}

impl Request {
    pub fn decode(from: &Peer, buff: &[u8], state: &mut State) -> Result<Self> {
        let data = RequestMsgData::decode(buff, state);
        Request::from_data(data?, from)
    }
    fn to_data(&self, id: Option<[u8; 32]>) -> Result<RequestMsgData> {
        Ok(RequestMsgData {
            tid: self.tid,
            to: self.to.clone(),
            id,
            internal: true,
            token: self.token,
            command: self.command,
            target: self.target,
            value: self.value.clone(),
        })
    }
    fn from_data(data: RequestMsgData, from: &Peer) -> Result<Self> {
        let mut new_from = from.clone();
        if let Some(id) = data.id {
            new_from.id = validate_id(&id, from);
        }
        Ok(Request {
            tid: data.tid,
            from: Some(new_from),
            to: data.to,
            token: data.token,
            internal: data.internal,
            command: data.command,
            target: data.target,
            value: data.value,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        tid: u16,
        from: Option<Peer>,
        to: Peer,
        token: Option<[u8; 32]>,
        internal: bool,
        command: Command,
        target: Option<[u8; 32]>,
        value: Option<Vec<u8>>,
    ) -> Self {
        Self {
            tid,
            from,
            to,
            token,
            internal,
            command,
            target,
            value,
        }
    }
    fn get_table_id(&self) -> [u8; 32] {
        todo!()
    }
}

type Inflight = Arc<RwLock<BTreeMap<u16, (Sender<Reply>, Request)>>>;

#[derive(Debug, Clone)]
pub enum Message {
    Request(Request),
    Reply(Reply),
}

fn on_message(inflight: &Inflight, buff: Vec<u8>, addr: SocketAddr) -> Result<()> {
    match decode_message(buff, addr)? {
        Message::Request(_r) => todo!(),
        Message::Reply(r) => {
            // check for tid and remove
            match inflight.write().unwrap().remove(&r.tid) {
                Some((sender, _req)) => {
                    let _ = sender.send(r);
                }
                None => warn!("Got reply for uknown tid: {}", r.tid),
            }
        }
    }
    Ok(())
}

pub fn decode_message(buff: Vec<u8>, addr: SocketAddr) -> Result<Message> {
    let mut state = State::new_with_start_and_end(1, buff.len());
    let from = Peer::from(&addr);
    Ok(match buff[0] {
        REQUEST_ID => Message::Request(Request::decode(&from, &buff, &mut state)?),
        RESPONSE_ID => Message::Reply(Reply::decode(&from, &buff, &mut state)?),
        _ => todo!("eror"),
    })
}
/// TODO hide secrets in fmt::Debug
#[derive(Debug)]
pub struct Secrets {
    rotate_secrets: usize,
    // NB starts null in js. Not initialized until token call.
    // so my behavior diverges when drain called until token
    // bc drain checks if secrets initialized
    secrets: [[u8; 32]; 2],
}

impl Secrets {
    pub fn new() -> Self {
        Self {
            rotate_secrets: 10,
            secrets: [thirty_two_random_bytes(), thirty_two_random_bytes()],
        }
    }
    fn rotate_secrets(&mut self) -> Result<()> {
        let tmp = self.secrets[0];
        self.secrets[0] = self.secrets[1];
        self.secrets[1] = generic_hash(&tmp);
        Ok(())
    }

    fn drain(&mut self) -> Result<()> {
        self.rotate_secrets -= 1;
        if self.rotate_secrets == 0 {
            self.rotate_secrets = 10;
            self.rotate_secrets()?;
        }
        Ok(())
    }

    pub fn token(&self, peer: &Peer, secret_index: usize) -> Result<[u8; 32]> {
        generic_hash_with_key(&ipv4(&peer.addr)?.octets()[..], &self.secrets[secret_index])
    }
}

#[derive(Debug, Clone)]
pub enum OutMessage {
    Request((Option<QueryId>, RequestMsgData)),
    Reply(ReplyMsgData),
}

impl OutMessage {
    fn to(&self) -> Peer {
        match self {
            OutMessage::Request((_, x)) => x.to.clone(),
            OutMessage::Reply(x) => x.to.clone(),
        }
    }
    fn inner(self) -> MsgData {
        match self {
            OutMessage::Request((_, x)) => MsgData::Request(x),
            OutMessage::Reply(x) => MsgData::Reply(x),
        }
    }
}

#[derive(Debug, Clone)]
struct InflightRequest {
    /// The message send
    message: RequestMsgData,
    /// The remote peer
    peer: Peer,
    /// Timestamp when the request was sent
    #[allow(unused)] // FIXME not read. Why not?
    timestamp: Instant,
    ///// Identifier for the query this request is used with
    query_id: Option<QueryId>,
}

#[derive(Debug)]
pub struct IoHandler {
    id: Slave<Key<IdBytes>>,
    ephemeral: bool,
    socket: MessageDataStream,
    /// Messages to send
    pending_send: VecDeque<OutMessage>,
    /// Current message
    pending_flush: Option<OutMessage>,
    /// Sent requests we currently wait for a response
    pending_recv: FnvHashMap<Tid, InflightRequest>,
    secrets: Secrets,
    tid: AtomicU16,
    rotation: Duration,
    last_rotation: Instant,
}

impl IoHandler {
    pub fn new(id: Slave<Key<IdBytes>>, socket: MessageDataStream, config: IoConfig) -> Self {
        Self {
            id,
            ephemeral: true,
            socket,
            pending_send: Default::default(),
            pending_flush: None,
            pending_recv: Default::default(),
            secrets: Secrets::new(),
            tid: AtomicU16::new(rand::thread_rng().gen()),
            rotation: config
                .rotation
                .unwrap_or_else(|| Duration::from_millis(ROTATE_INTERVAL)),
            last_rotation: Instant::now(),
        }
    }
    pub fn is_ephemeral(&self) -> bool {
        self.ephemeral
    }

    pub fn local_addr(&self) -> crate::Result<SocketAddr> {
        self.socket.local_addr()
    }
    /// TODO check this is correct.
    fn token(&self, peer: &Peer, secret_index: usize) -> crate::Result<[u8; 32]> {
        self.secrets.token(peer, secret_index)
    }

    fn request(&mut self, ev: OutMessage) {
        self.pending_send.push_back(ev)
    }

    pub fn query(
        &mut self,
        command: udx::Command,
        target: Option<[u8; 32]>,
        value: Option<Vec<u8>>,
        peer: Peer,
        query_id: Option<QueryId>,
    ) -> QueryAndTid {
        let id = if !self.ephemeral {
            Some(self.id.get().preimage().0)
        } else {
            None
        };

        let tid = self.tid.fetch_add(1, Ordering::Relaxed);
        self.request(OutMessage::Request((
            query_id,
            RequestMsgData {
                tid,
                to: peer,
                id,
                internal: true,
                token: None,
                command,
                target,
                value,
            },
        )));
        (query_id, tid)
    }
    pub fn error(
        &mut self,
        request: RequestMsgData,
        error: usize,
        value: Option<Vec<u8>>,
        closer_nodes: Option<Vec<Peer>>,
        peer: &Peer,
    ) -> crate::Result<()> {
        let id = if !self.ephemeral {
            Some(self.id.get().preimage().0)
        } else {
            None
        };

        let token = Some(self.token(peer, 1)?);

        self.pending_send.push_back(OutMessage::Reply(ReplyMsgData {
            tid: request.tid,
            to: peer.clone(),
            id,
            token,
            closer_nodes: closer_nodes.unwrap_or_default(),
            error,
            value,
        }));
        Ok(())
    }

    pub fn reply(&mut self, msg: ReplyMsgData) {
        self.pending_send.push_back(OutMessage::Reply(msg))
    }

    pub fn response(
        &mut self,
        request: RequestMsgData,
        value: Option<Vec<u8>>,
        closer_nodes: Option<Vec<Peer>>,
        peer: Peer,
    ) -> crate::Result<()> {
        let id = if !self.ephemeral {
            Some(self.id.get().preimage().0)
        } else {
            None
        };
        let token = Some(self.token(&peer, 1)?);
        self.pending_send.push_back(OutMessage::Reply(ReplyMsgData {
            tid: request.tid,
            to: peer.clone(),
            id,
            token,
            closer_nodes: closer_nodes.unwrap_or_default(),
            error: 0,
            value,
        }));
        Ok(())
    }
    fn on_response(&mut self, recv: ReplyMsgData, peer: Peer) -> IoHandlerEvent {
        if let Some(req) = self.pending_recv.remove(&recv.tid) {
            return IoHandlerEvent::InResponse {
                peer,
                resp: recv,
                req: Box::new(req.message),
                query_id: req.query_id,
            };
        }
        IoHandlerEvent::InResponseBadRequestId {
            peer,
            message: recv,
        }
    }
    /// A new `Message` was read from the socket.
    fn on_message(&mut self, msg: MsgData, rinfo: SocketAddr) -> IoHandlerEvent {
        let peer = Peer::from(&rinfo);
        match msg {
            MsgData::Request(req) => IoHandlerEvent::InRequest { message: req, peer },
            MsgData::Reply(rep) => self.on_response(rep, peer),
        }
    }

    fn start_send_next(&mut self) -> crate::Result<()> {
        if self.pending_flush.is_none() {
            if let Some(msg) = self.pending_send.pop_front() {
                //log::trace!("send to {}: {}", peer.addr, msg);
                let addr = SocketAddr::from(&msg.to());

                Sink::start_send(Pin::new(&mut self.socket), (msg.clone().inner(), addr))?;
                self.pending_flush = Some(msg);
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
pub struct IoConfig {
    pub rotation: Option<Duration>,
    pub secrets: Option<([u8; 32], [u8; 32])>,
}

impl Stream for IoHandler {
    type Item = IoHandlerEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        // queue in the next message if not currently flushing
        // moves msg pending_flush = pending_send[0]
        // and sends it
        if let Err(err) = pin.start_send_next() {
            let out = IoHandlerEvent::OutSocketErr { err };
            trace!("{out:#?}");
            return Poll::Ready(Some(out));
        }

        // flush the message
        if let Some(ev) = pin.pending_flush.take() {
            if Sink::poll_ready(Pin::new(&mut pin.socket), cx).is_ready() {
                return match ev {
                    OutMessage::Request((query_id, message)) => {
                        let tid = message.tid;
                        let peer = message.to.clone();
                        pin.pending_recv.insert(
                            message.tid,
                            InflightRequest {
                                message,
                                peer,
                                timestamp: Instant::now(),
                                query_id,
                            },
                        );
                        let out = IoHandlerEvent::OutRequest { tid };
                        trace!("{out:#?}");
                        return Poll::Ready(Some(out));
                    }
                    OutMessage::Reply(message) => {
                        let peer = message.to.clone();
                        let out = IoHandlerEvent::OutResponse { message, peer };
                        trace!("{out:#?}");
                        return Poll::Ready(Some(out));
                    }
                };
            } else {
                pin.pending_flush = Some(ev);
            }
        }

        // read from socket
        match Stream::poll_next(Pin::new(&mut pin.socket), cx) {
            Poll::Ready(Some(Ok((msg, rinfo)))) => {
                let out = pin.on_message(msg, rinfo);
                trace!("{out:#?}");
                return Poll::Ready(Some(out));
            }
            Poll::Ready(Some(Err(err))) => {
                let out = IoHandlerEvent::InSocketErr { err };
                trace!("{out:#?}");
                return Poll::Ready(Some(out));
            }
            _ => {}
        }

        //if pin.last_rotation + pin.rotation > Instant::now() {
        //    pin.rotate_secrets();
        //}

        Poll::Pending
    }
}

/// Event generated by the IO handler
#[derive(Debug)]
pub enum IoHandlerEvent {
    ///  A response was sent
    OutResponse { message: ReplyMsgData, peer: Peer },
    /// A request was sent
    OutRequest { tid: Tid },
    /// A Response to a Query Message was recieved
    InResponse {
        req: Box<RequestMsgData>,
        resp: ReplyMsgData,
        peer: Peer,
        query_id: Option<QueryId>,
    },
    /// A Request was receieved
    InRequest { message: RequestMsgData, peer: Peer },
    /// Error while sending a message
    OutSocketErr { err: crate::Error },
    /// A request did not recieve a response within the given timeout
    RequestTimeout {
        message: MsgData,
        peer: Peer,
        sent: Instant,
        query_id: QueryId,
    },
    /// Error while decoding a message from socket
    /// TODO unused
    InMessageErr { err: io::Error, peer: Peer },
    /// Error while reading from socket
    InSocketErr { err: crate::Error },
    /// Received a response with a request id that was doesn't match any pending
    /// responses.
    InResponseBadRequestId { message: ReplyMsgData, peer: Peer },
}

#[cfg(test)]
mod test {
    use futures::StreamExt;
    use udx::{mslave::Master, thirty_two_random_bytes, InternalCommand};

    use super::*;

    fn new_io() -> IoHandler {
        let view = Master::new(Key::new(IdBytes::from(thirty_two_random_bytes()))).view();
        let socket = MessageDataStream::defualt_bind().unwrap();
        IoHandler::new(view, socket, Default::default())
    }
    #[tokio::test]
    async fn foo() -> crate::Result<()> {
        let mut a = new_io();
        let mut b = new_io();

        let to = Peer::from(&b.local_addr()?);
        let id = Some(thirty_two_random_bytes());
        let msg = RequestMsgData {
            tid: 42,
            to,
            id,
            internal: true,
            token: None,
            command: InternalCommand::Ping.into(),
            target: None,
            value: None,
        };
        let query_id = Some(QueryId(42));
        a.request(OutMessage::Request((query_id, msg.clone())));
        a.next().await;
        let IoHandlerEvent::InRequest { message: res, .. } = b.next().await.unwrap() else {
            panic!()
        };
        assert_eq!(res, msg);
        Ok(())
    }
}
