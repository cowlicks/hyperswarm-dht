use crate::{
    kbucket::Key,
    peers::PeersEncoding,
    rpc::{
        fill_random_bytes,
        message::{Command, Holepunch, Message, Type},
        protocol::DhtRpcCodec,
        udp::UdpFramed,
        IdBytes, Peer, RequestId,
    },
    udx,
};
use blake2::{
    crypto_mac::generic_array::{typenum::U64, GenericArray},
    Blake2b, Digest,
};
use fnv::FnvHashMap;
use futures::Stream;
use futures::{
    task::{Context, Poll},
    Sink,
};
use prost::Message as ProtoMessage;
use rand::Rng;
use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt, io,
    net::SocketAddr,
    ops::Deref,
    pin::Pin,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::net::UdpSocket;
use wasm_timer::Instant;

use super::{
    cenc::{MsgData, ReplyMsgData, RequestMsgData},
    stream::MessageDataStream,
    Addr,
};

pub const VERSION: u64 = 1;

const ROTATE_INTERVAL: u64 = 300_000;

type Tid = u16;

#[derive(Debug, Clone)]
struct InflightRequest<TUserData: fmt::Debug + Clone> {
    /// The message send
    message: Message,
    /// The remote peer
    peer: Peer,
    /// Timestamp when the request was sent
    #[allow(unused)] // FIXME not read. Why not?
    timestamp: Instant,
    user_data: TUserData,
}

impl<TUserData: fmt::Debug + Clone> InflightRequest<TUserData> {
    fn into_event(self) -> Result<MessageEvent<TUserData>, Self> {
        if let Ok(ty) = self.message.get_type() {
            match ty {
                Type::Query => Ok(MessageEvent::Query {
                    msg: self.message,
                    peer: self.peer,
                    user_data: self.user_data,
                }),
                Type::Update => Ok(MessageEvent::Update {
                    msg: self.message,
                    peer: self.peer,
                    user_data: self.user_data,
                }),
                _ => Err(self),
            }
        } else {
            Err(self)
        }
    }
}

#[derive(Debug, Clone)]
struct InflightRequest2 {
    /// The message send
    message: MsgData,
    /// The remote peer
    peer: Addr,
    /// Timestamp when the request was sent
    #[allow(unused)] // FIXME not read. Why not?
    timestamp: Instant,
}

impl InflightRequest2 {
    fn into_event(self) -> Result<MsgData, Self> {
        Ok(self.message)
    }
}

#[derive(Debug)]
pub enum MessageEvent<TUserData: fmt::Debug + Clone> {
    Update {
        msg: Message,
        peer: Peer,
        user_data: TUserData,
    },
    Query {
        msg: Message,
        peer: Peer,
        user_data: TUserData,
    },
    Response {
        msg: Message,
        peer: Peer,
    },
    // TODO FireAndForget like Response?
}

impl<TUserData: fmt::Debug + Clone> MessageEvent<TUserData> {
    fn inner(&self) -> (&Message, &Peer) {
        match self {
            MessageEvent::Update { peer, msg, .. } => (msg, peer),
            MessageEvent::Query { peer, msg, .. } => (msg, peer),
            MessageEvent::Response { peer, msg } => (msg, peer),
        }
    }

    fn inner_mut(&mut self) -> (&mut Message, &mut Peer) {
        match self {
            MessageEvent::Update { peer, msg, .. } => (msg, peer),
            MessageEvent::Query { peer, msg, .. } => (msg, peer),
            MessageEvent::Response { peer, msg } => (msg, peer),
        }
    }
}

#[derive(Debug)]
pub struct IoHandler<TUserData: fmt::Debug + Clone> {
    id: Option<Key<IdBytes>>,
    socket: UdpFramed<DhtRpcCodec>,
    /// Messages to send
    pending_send: VecDeque<MessageEvent<TUserData>>,
    /// Current message
    pending_flush: Option<MessageEvent<TUserData>>,
    /// Sent requests we currently wait for a response
    pending_recv: FnvHashMap<RequestId, InflightRequest<TUserData>>,
    secrets: ([u8; 32], [u8; 32]),

    next_req_id: RequestId,

    rotation: Duration,
    last_rotation: Instant,
}

use super::io::Secrets;

#[derive(Debug)]
pub struct IoHandler2 {
    id: Arc<RefCell<[u8; 32]>>,
    ephemeral: bool,
    socket: MessageDataStream,
    /// Messages to send
    pending_send: VecDeque<MsgData>,
    /// Current message
    pending_flush: Option<MsgData>,
    /// Sent requests we currently wait for a response
    pending_recv: FnvHashMap<Tid, InflightRequest2>,
    secrets: Secrets,
    tid: AtomicU16,
    rotation: Duration,
    last_rotation: Instant,
}

impl IoHandler2 {
    pub fn new(id: Arc<RefCell<[u8; 32]>>, socket: MessageDataStream, config: IoConfig) -> Self {
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
        Ok(self.socket.local_addr()?)
    }
    /// TODO check this is correct.
    fn token(&self, peer: &Addr, secret_index: usize) -> crate::Result<[u8; 32]> {
        self.secrets.token(peer, secret_index)
    }

    fn request(&mut self, ev: MsgData) {
        self.pending_send.push_back(ev)
    }

    pub fn query(
        &mut self,
        command: udx::Command,
        target: Option<[u8; 32]>,
        value: Option<Vec<u8>>,
        peer: Addr,
    ) {
        let id = if !self.ephemeral {
            Some(*self.id.borrow())
        } else {
            None
        };

        self.request(MsgData::Request(RequestMsgData {
            tid: self.tid.fetch_add(1, Ordering::Relaxed),
            to: peer,
            id,
            internal: true,
            token: None,
            command,
            target,
            value,
        }));
    }
    pub fn error(
        &mut self,
        request: RequestMsgData,
        error: usize,
        value: Option<Vec<u8>>,
        closer_nodes: Option<Vec<Addr>>,
        peer: &Addr,
    ) -> crate::Result<()> {
        let id = if !self.ephemeral {
            Some(*self.id.borrow())
        } else {
            None
        };

        let token = Some(self.token(peer, 1)?);

        self.pending_send.push_back(MsgData::Reply(ReplyMsgData {
            tid: request.tid,
            to: peer.clone(),
            id,
            token,
            closer_nodes: closer_nodes.unwrap_or_else(|| Vec::new()),
            error,
            value,
        }));
        Ok(())
    }
    pub fn reply(&mut self, mut _msg: RequestMsgData, _peer: Addr) {
        todo!()
    }
    pub fn response(
        &mut self,
        request: RequestMsgData,
        value: Option<Vec<u8>>,
        closer_nodes: Option<Vec<Addr>>,
        peer: Addr,
    ) -> crate::Result<()> {
        let id = if !self.ephemeral {
            Some(*self.id.borrow())
        } else {
            None
        };
        let token = Some(self.token(&peer, 1)?);
        self.pending_send.push_back(MsgData::Reply(ReplyMsgData {
            tid: request.tid,
            to: peer.clone(),
            id,
            token,
            closer_nodes: closer_nodes.unwrap_or_else(|| Vec::new()),
            error: 0,
            value,
        }));
        Ok(())
    }
    fn on_response(&mut self, recv: ReplyMsgData, peer: Addr) -> IoHandlerEvent2 {
        if let Some(req) = self.pending_recv.remove(&recv.tid) {
            return IoHandlerEvent2::InResponse {
                peer,
                resp: recv,
                req: Box::new(req.message),
            };
        }
        IoHandlerEvent2::InResponseBadRequestId { peer, msg: recv }
    }
    /// A new `Message` was read from the socket.
    fn on_message(&mut self, msg: MsgData, rinfo: SocketAddr) -> IoHandlerEvent2 {
        let peer = Addr::from(&rinfo);
        match msg {
            MsgData::Request(req) => IoHandlerEvent2::InRequest { msg: req, peer },
            MsgData::Reply(rep) => self.on_response(rep, peer),
        }
    }

    fn start_send_next(&mut self) -> crate::Result<()> {
        if self.pending_flush.is_none() {
            if let Some(msg) = self.pending_send.pop_front() {
                //log::trace!("send to {}: {}", peer.addr, msg);
                let addr = SocketAddr::from(&msg.to());

                Sink::start_send(Pin::new(&mut self.socket), (msg.clone(), addr))?;
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

impl<TUserData> IoHandler<TUserData>
where
    TUserData: fmt::Debug + Clone,
{
    pub fn new(
        id: Option<Key<IdBytes>>,
        socket: UdpSocket,
        config: IoConfig,
    ) -> IoHandler<TUserData> {
        let socket = UdpFramed::new(socket, DhtRpcCodec::default());

        let secrets = config.secrets.unwrap_or_else(|| {
            let mut k1 = [0; 32];
            let mut k2 = [0; 32];
            fill_random_bytes(&mut k1);
            fill_random_bytes(&mut k2);
            (k1, k2)
        });

        Self {
            id,
            socket,
            pending_send: Default::default(),
            pending_flush: None,
            pending_recv: Default::default(),
            secrets,
            next_req_id: Self::random_id(),
            rotation: config
                .rotation
                .unwrap_or_else(|| Duration::from_millis(ROTATE_INTERVAL)),
            last_rotation: Instant::now(),
        }
    }

    fn random_id() -> RequestId {
        use rand::Rng;
        RequestId(rand::thread_rng().gen_range(0, u16::MAX as u64))
    }

    // msg.id not read?
    #[inline]
    pub(crate) fn msg_id(&self) -> Option<Vec<u8>> {
        self.id.as_ref().map(|k| k.preimage().clone().to_vec())
    }

    #[inline]
    pub fn is_ephemeral(&self) -> bool {
        self.id.is_some()
    }

    /// Generate the next request id
    #[inline]
    fn next_req_id(&mut self) -> RequestId {
        let rid = self.next_req_id;
        self.next_req_id = RequestId(self.next_req_id.0.wrapping_add(1));
        rid
    }

    /// Returns the local address that this listener is bound to.
    #[inline]
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.get_ref().local_addr()
    }

    /// Generate a blake2 hash based on the peer's ip and the provided secret
    fn token(&self, peer: &Peer, secret: &[u8]) -> GenericArray<u8, U64> {
        let mut context = Blake2b::new();
        context.update(secret);
        context.update(peer.addr.ip().to_string().as_bytes());
        context.finalize()
    }

    fn holepunch(
        &mut self,
        mut msg: Message,
        peer: SocketAddr,
        referrer: SocketAddr,
        user_data: TUserData,
    ) {
        msg.version = Some(VERSION);
        msg.r#type = Type::Query.id();
        msg.to = Some(referrer.encode());
        msg.set_holepunch(&Holepunch::with_to(peer.encode()));
        self.send_message(MessageEvent::Query {
            msg,
            peer: Peer::from(referrer),
            user_data,
        })
    }

    fn request(&mut self, mut ev: MessageEvent<TUserData>) {
        let (msg, peer) = ev.inner_mut();
        msg.rid = self.next_req_id().0;

        if msg.is_holepunch() && peer.referrer.is_some() {
            match &ev {
                MessageEvent::Update {
                    msg,
                    peer,
                    user_data,
                }
                | MessageEvent::Query {
                    msg,
                    peer,
                    user_data,
                } => self.holepunch(
                    msg.clone(),
                    peer.addr,
                    peer.referrer.unwrap(),
                    user_data.clone(),
                ),
                _ => {}
            }
        }
        self.pending_send.push_back(ev)
    }

    /// Send a new Query message
    pub fn query(
        &mut self,
        cmd: Command,
        target: Option<IdBytes>,
        value: Option<Vec<u8>>,
        peer: Peer,
        user_data: TUserData,
    ) {
        let msg = Message {
            version: Some(VERSION),
            r#type: Type::Query.id(),
            rid: 0,
            to: Some(peer.encode()),
            id: self.msg_id(),
            target: target.map(|x| x.to_vec()),
            closer_nodes: None,
            roundtrip_token: None,
            command: Some(cmd.to_string()),
            error: None,
            value,
        };

        self.request(MessageEvent::Query {
            msg,
            peer,
            user_data,
        })
    }

    pub fn error(
        &mut self,
        request: Message,
        error: String,
        value: Option<Vec<u8>>,
        closer_nodes: Option<Vec<u8>>,
        peer: Peer,
    ) {
        let msg = Message {
            version: Some(VERSION),
            r#type: Type::Response.id(),
            rid: request.rid,
            to: Some(peer.encode()),
            id: self.msg_id(),
            target: None,
            closer_nodes,
            roundtrip_token: None,
            command: None,
            error: Some(error),
            value,
        };
        self.pending_send
            .push_back(MessageEvent::Response { msg, peer })
    }

    pub fn reply(&mut self, mut msg: Message, peer: Peer) {
        msg.id = self.msg_id();
        if msg.error.is_none() {
            msg.roundtrip_token = Some(self.token(&peer, &self.secrets.0[..]).to_vec());
        }
        self.pending_send
            .push_back(MessageEvent::Response { msg, peer })
    }

    pub fn response(
        &mut self,
        request: Message,
        value: Option<Vec<u8>>,
        closer_nodes: Option<Vec<u8>>,
        peer: Peer,
    ) {
        let msg = Message {
            version: Some(VERSION),
            r#type: Type::Response.id(),
            rid: request.rid,
            to: Some(peer.encode()),
            id: self.msg_id(),
            target: None,
            closer_nodes,
            roundtrip_token: Some(self.token(&peer, &self.secrets.0[..]).to_vec()),
            command: None,
            error: None,
            value,
        };
        self.pending_send
            .push_back(MessageEvent::Response { msg, peer })
    }

    pub fn send_message(&mut self, msg: MessageEvent<TUserData>) {
        self.pending_send.push_back(msg)
    }

    /// Send an update message
    pub fn update(
        &mut self,
        cmd: Command,
        target: Option<IdBytes>,
        value: Option<Vec<u8>>,
        peer: Peer,
        roundtrip_token: Option<Vec<u8>>,
        user_data: TUserData,
    ) {
        let msg = Message {
            version: Some(VERSION),
            r#type: Type::Update.id(),
            rid: 0,
            to: Some(peer.encode()),
            id: self.msg_id(),
            target: target.map(|x| x.to_vec()),
            closer_nodes: None,
            roundtrip_token,
            command: Some(cmd.to_string()),
            error: None,
            value,
        };

        self.request(MessageEvent::Update {
            msg,
            peer,
            user_data,
        });
    }

    fn on_response(&mut self, recv: Message, peer: Peer) -> IoHandlerEvent<TUserData> {
        if let Some(req) = self.pending_recv.remove(&recv.get_request_id()) {
            return IoHandlerEvent::InResponse {
                peer,
                resp: recv,
                req: Box::new(req.message),
                user_data: req.user_data,
            };
        }
        IoHandlerEvent::InResponseBadRequestId { peer, msg: recv }
    }

    /// A new `Message` was read from the socket.
    fn on_message(
        &mut self,
        mut msg: Message,
        rinfo: SocketAddr,
    ) -> Option<IoHandlerEvent<TUserData>> {
        log::trace!("recv from {}: {}", rinfo, msg);
        if let Some(ref id) = msg.id {
            if id.len() != 32 {
                // TODO Receive Error? clear waiting?
                return None;
            }
            // Force eph if older version
            if msg.version.unwrap_or_default() < VERSION {
                msg.id = None
            }
        }
        let peer = Peer::from(rinfo);

        match msg.get_type() {
            Ok(ty) => match ty {
                Type::Response => Some(self.on_response(msg, peer)),
                Type::Query => Some(IoHandlerEvent::InRequest {
                    peer,
                    msg,
                    ty: Type::Query,
                }),
                Type::Update => {
                    if let Some(ref rt) = msg.roundtrip_token {
                        if rt.as_slice() != self.token(&peer, &self.secrets.0).deref()
                            && rt.as_slice() != self.token(&peer, &self.secrets.1).deref()
                        {
                            None
                        } else {
                            Some(IoHandlerEvent::InRequest {
                                peer,
                                msg,
                                ty: Type::Update,
                            })
                        }
                    } else {
                        None
                    }
                }
            },
            Err(_) => {
                // TODO handle unknown type?
                None
            }
        }
    }

    #[inline]
    fn rotate_secrets(&mut self) {
        std::mem::swap(&mut self.secrets.0, &mut self.secrets.1);
        self.last_rotation = Instant::now()
    }

    /// Remove the matching request from the sending queue or stop waiting for a
    /// response if already sent.
    pub fn cancel(
        &mut self,
        rid: RequestId,
        _error: Option<String>,
    ) -> Option<MessageEvent<TUserData>> {
        if let Some(s) = self
            .pending_send
            .iter()
            .position(|e| e.inner().0.get_request_id() == rid)
        {
            self.pending_send.remove(s)
        } else {
            self.pending_recv
                .remove(&rid)
                .and_then(|req| req.into_event().ok())
        }
    }

    fn start_send_next(&mut self) -> io::Result<()> {
        if self.pending_flush.is_none() {
            if let Some(event) = self.pending_send.pop_front() {
                let (msg, peer) = event.inner();
                log::trace!("send to {}: {}", peer.addr, msg);
                let mut buf = Vec::with_capacity(msg.encoded_len());
                msg.encode(&mut buf)?;
                Sink::start_send(Pin::new(&mut self.socket), (buf, peer.addr))?;
                self.pending_flush = Some(event);
            }
        }
        Ok(())
    }
}

impl<TUserData: Unpin + fmt::Debug + Clone> Stream for IoHandler<TUserData> {
    type Item = IoHandlerEvent<TUserData>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        // queue in the next message if not currently flushing
        // moves msg pending_flush = pending_send[0]
        // and sends it
        if let Err(err) = pin.start_send_next() {
            return Poll::Ready(Some(IoHandlerEvent::OutSocketErr { err }));
        }

        // flush the message
        if let Some(ev) = pin.pending_flush.take() {
            if Sink::poll_ready(Pin::new(&mut pin.socket), cx).is_ready() {
                return match ev {
                    MessageEvent::Update {
                        msg,
                        peer,
                        user_data,
                    }
                    | MessageEvent::Query {
                        msg,
                        peer,
                        user_data,
                    } => {
                        let id = msg.get_request_id();
                        pin.pending_recv.insert(
                            id,
                            InflightRequest {
                                message: msg,
                                peer,
                                timestamp: Instant::now(),
                                user_data,
                            },
                        );
                        return Poll::Ready(Some(IoHandlerEvent::OutRequest { id }));
                    }
                    MessageEvent::Response { msg, peer } => {
                        Poll::Ready(Some(IoHandlerEvent::OutResponse { msg, peer }))
                    }
                };
            } else {
                pin.pending_flush = Some(ev);
            }
        }

        // read from socket
        match Stream::poll_next(Pin::new(&mut pin.socket), cx) {
            Poll::Ready(Some(Ok((msg, rinfo)))) => {
                if let Some(event) = pin.on_message(msg, rinfo) {
                    return Poll::Ready(Some(event));
                }
            }
            Poll::Ready(Some(Err(err))) => {
                return Poll::Ready(Some(IoHandlerEvent::InSocketErr { err }));
            }
            _ => {}
        }

        if pin.last_rotation + pin.rotation > Instant::now() {
            pin.rotate_secrets();
        }

        Poll::Pending
    }
}

/// Event generated by the IO handler
#[derive(Debug)]
pub enum IoHandlerEvent<TUserData> {
    ///  A response was sent
    OutResponse { msg: Message, peer: Peer },
    /// A request was sent
    OutRequest { id: RequestId },
    /// A Response to a Message was recieved
    InResponse {
        req: Box<Message>,
        resp: Message,
        peer: Peer,
        user_data: TUserData,
    },
    /// A Request was receieved
    InRequest { msg: Message, peer: Peer, ty: Type },
    /// Error while sending a message
    OutSocketErr { err: io::Error },
    /// A request did not recieve a response within the given timeout
    RequestTimeout {
        msg: Message,
        peer: Peer,
        sent: Instant,
        user_data: TUserData,
    },
    /// Error while decoding a message from socket
    /// TODO unused
    InMessageErr { err: io::Error, peer: Peer },
    /// Error while reading from socket
    InSocketErr { err: io::Error },
    /// Received a response with a request id that was doesn't match any pending
    /// responses.
    InResponseBadRequestId { msg: Message, peer: Peer },
}

/// Event generated by the IO handler
#[derive(Debug)]
pub enum IoHandlerEvent2 {
    ///  A response was sent
    OutResponse { msg: MsgData, peer: Addr },
    /// A request was sent
    OutRequest { id: Tid },
    /// A Response to a Message was recieved
    InResponse {
        req: Box<MsgData>,
        resp: ReplyMsgData,
        peer: Addr,
    },
    /// A Request was receieved
    InRequest { msg: RequestMsgData, peer: Addr },
    /// Error while sending a message
    OutSocketErr { err: io::Error },
    /// A request did not recieve a response within the given timeout
    RequestTimeout {
        msg: MsgData,
        peer: Addr,
        sent: Instant,
    },
    /// Error while decoding a message from socket
    /// TODO unused
    InMessageErr { err: io::Error, peer: Addr },
    /// Error while reading from socket
    InSocketErr { err: io::Error },
    /// Received a response with a request id that was doesn't match any pending
    /// responses.
    InResponseBadRequestId { msg: ReplyMsgData, peer: Addr },
}
