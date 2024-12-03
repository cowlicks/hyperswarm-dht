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
    net::SocketAddr,
    pin::Pin,
    sync::atomic::{AtomicU16, Ordering},
    time::Duration,
};
use tracing::trace;
use wasm_timer::Instant;

use super::message::{MsgData, ReplyMsgData, RequestMsgData};
use super::mslave::Slave;
use super::smod::Peer;
use super::{io::Secrets, stream::MessageDataStream};

pub const VERSION: u64 = 1;

const ROTATE_INTERVAL: u64 = 300_000;

type Tid = u16;

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
    ) {
        let id = if !self.ephemeral {
            Some(self.id.get().preimage().0)
        } else {
            None
        };

        self.request(OutMessage::Request((
            query_id,
            RequestMsgData {
                tid: self.tid.fetch_add(1, Ordering::Relaxed),
                to: peer,
                id,
                internal: true,
                token: None,
                command,
                target,
                value,
            },
        )));
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
    pub fn reply(&mut self, mut _msg: RequestMsgData, _peer: Peer) {
        todo!()
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
