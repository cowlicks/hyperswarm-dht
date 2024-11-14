use async_udx::UdxSocket;
use log::warn;
use rand::Rng;
#[allow(unreachable_code, dead_code)]
use std::sync::{Arc, RwLock};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::atomic::{AtomicU16, Ordering},
};
use tokio::sync::{
    oneshot::{channel, Receiver, Sender},
    RwLock as TRwLock,
};

use crate::{
    constants::{ID_SIZE, REQUEST_ID, RESPONSE_ID},
    Result,
};
use compact_encoding::{CompactEncoding, State};

use super::{
    cenc::{decode_reply, decode_request},
    Addr, Command,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Reply {
    pub tid: u16,
    pub rtt: usize,
    pub from: Addr,
    pub to: Addr,
    pub token: Option<[u8; 32]>,
    pub closer_nodes: Option<Vec<Addr>>,
    pub error: u8,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub struct Request {
    pub tid: u16,
    pub from: Option<Addr>,
    // TODO remove this field
    pub to: Option<Addr>,
    pub token: Option<[u8; 32]>,
    pub internal: bool,
    pub command: Command,
    pub target: Option<[u8; 32]>,
    pub value: Option<Vec<u8>>,
}

impl Request {
    fn new(
        tid: u16,
        from: Option<Addr>,
        to: Option<Addr>,
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
    pub fn encode_request(&self, io: &Io, is_server_socket: bool) -> Result<Vec<u8>> {
        // this useses...
        // io.ephemeral
        // is_server_socket
        //
        // REQUEST_ID
        // flags: {
        //  id | token | internal | target | value
        //  }
        //  tid
        //  to
        //  table_id
        //
        //  token
        //  command
        //  target
        //  value
        let id = !io.ephemeral && is_server_socket;
        let mut state = State::new();
        state.add_end(1 + 1 + 6 + 2)?;

        if is_server_socket {
            state.add_end(ID_SIZE)?;
        }
        if self.token.is_some() {
            state.add_end(32)?;
        }

        let cmd = self.command.clone() as usize;
        state.preencode_usize_var(&cmd)?;

        if self.target.is_some() {
            state.add_end(32)?;
        }

        if let Some(v) = &self.value {
            state.preencode_buffer(v)?;
        }

        let mut buff = state.create_buffer();

        buff[state.start()] = REQUEST_ID;
        state.add_start(1)?;

        let mut flags: u8 = 0;
        if id {
            flags |= 1 << 0;
        }
        if self.token.is_some() {
            flags |= 1 << 1;
        }
        if self.internal {
            flags |= 1 << 2;
        }
        if self.target.is_some() {
            flags |= 1 << 3;
        }
        if self.value.is_some() {
            flags |= 1 << 4;
        }
        buff[state.start()] = flags;
        state.add_start(1)?;

        // TODO add state.encode_u16 function to compact_encoding
        state.encode_u16(self.tid, &mut buff)?;

        let Some(to) = self.to.as_ref() else {
            // Maybe Req should have *one* field for To/From that's just Addr
            todo!()
        };
        state.encode(to, &mut buff)?;

        if id {
            state.encode_fixed_32(&self.get_table_id(), &mut buff)?;
        }
        if let Some(t) = self.token {
            // c.fixed32.encode(state, token)
            state.encode_fixed_32(&t, &mut buff)?;
        }

        buff[state.start()] = self.command.clone() as u8;
        state.add_start(1)?;

        // c.uint.encode(state, this.command)
        if let Some(t) = &self.target {
            // c.fixed32.encode(state, this.target)
            state.encode_fixed_32(t, &mut buff)?;
        }
        if let Some(v) = self.value.as_ref() {
            state.encode_buffer(v, &mut buff)?;
        }
        println!(
            "Request {{
          flags = {{
              id = {id},
              token? = {},
              internal = {},
              target? = {},
              value? = {}
          }}
          flags = {flags}
          tid = {},
          to = {:?},
          table_id = TODO 
          token = {},
          command = {},
          target = {},
          value = {},
        }}",
            self.token.is_some(),
            self.internal,
            self.target.is_some(),
            self.value.is_some(),
            self.tid,
            self.to
                .as_ref()
                .map(|x| SocketAddr::from(x).to_string())
                .unwrap_or("null".to_string()),
            //self.get_table_id(),
            self.token
                .as_ref()
                .map(|x| pretty_hash::fmt(x).unwrap())
                .unwrap_or("null".to_string())
                .to_string(),
            self.command,
            self.target
                .as_ref()
                .map(|x| pretty_hash::fmt(x).unwrap())
                .unwrap_or("null".to_string()),
            self.value
                .as_ref()
                .map(|x| pretty_hash::fmt(x).unwrap())
                .unwrap_or("null".to_string())
        );

        // use prettyhash
        Ok(buff.into())
    }
}

type Inflight = Arc<RwLock<BTreeMap<u16, (Sender<Reply>, Request)>>>;
#[derive(Debug)]
pub struct Io {
    tid: AtomicU16,
    ephemeral: bool,
    socket: Arc<TRwLock<UdxSocket>>,
    inflight: Inflight,
}

pub enum Message {
    Request(Request),
    Reply(Reply),
}

impl Io {
    pub fn new() -> Result<Self> {
        let socket = Arc::new(TRwLock::new(UdxSocket::bind("0.0.0.0:0")?));
        let recv_socket = socket.clone();
        let inflight: Inflight = Default::default();
        let _recv_inflight = inflight.clone();
        tokio::spawn(async move {
            loop {
                // TODO add timeout so rw is not locked forever
                let x = recv_socket.read().await.recv().await;
                if let Ok((addr, buff)) = x {
                    dbg!(&addr);
                    dbg!(pretty_hash::fmt(&buff).unwrap());
                    on_message(&_recv_inflight, buff, addr)?
                }
            }
            Ok::<(), crate::Error>(())
        });
        Ok(Self {
            tid: AtomicU16::new(rand::thread_rng().gen()),
            ephemeral: true,
            socket,
            inflight,
        })
    }
    pub fn create_ping(&self, to: &Addr) -> Request {
        self.create_request(to, None, true, Command::Ping, None, None)
    }

    pub fn create_ping_nat(&self, to: &Addr, value: Vec<u8>) -> Request {
        self.create_request(to, None, true, Command::PingNat, None, Some(value))
    }

    pub fn create_find_node(&self, to: &Addr, target: &[u8; 32]) -> Request {
        self.create_request(to, None, true, Command::FindNode, Some(*target), None)
    }

    pub async fn send_find_node(&self, to: &Addr, target: &[u8; 32]) -> Result<Receiver<Reply>> {
        let req = self.create_find_node(to, target);
        self.send(to, req).await
    }

    pub async fn send(&self, to: &Addr, request: Request) -> Result<Receiver<Reply>> {
        let buff = request.encode_request(self, false)?;
        let socket_addr = SocketAddr::from(to);
        let (tx, rx) = channel();
        self.inflight
            .write()
            .unwrap()
            .insert(request.tid, (tx, request));
        self.socket.read().await.send(socket_addr, &buff);
        Ok(rx)
    }

    fn create_request(
        &self,
        to: &Addr,
        token: Option<[u8; 32]>,
        internal: bool,
        command: Command,
        target: Option<[u8; 32]>,
        value: Option<Vec<u8>>,
    ) -> Request {
        let tid = self.tid.fetch_add(1, Ordering::Relaxed);
        Request::new(
            tid,
            None,
            Some(to.clone()),
            token,
            internal,
            command,
            target,
            value,
        )
    }

    // js Io::onresponse
    fn on_reply(&self, _reply: Reply) -> Result<()> {
        todo!()
    }
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
    let from = Addr::from(&addr);
    Ok(match buff[0] {
        REQUEST_ID => Message::Request(decode_request(&buff, from, &mut state)?),
        RESPONSE_ID => Message::Reply(decode_reply(&buff, from, &mut state)?),
        _ => todo!("eror"),
    })
}
