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
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    oneshot::{channel, Receiver, Sender},
    RwLock as TRwLock,
};

use crate::{
    constants::{ID_SIZE, REQUEST_ID, RESPONSE_ID},
    Result,
};
use compact_encoding::{CompactEncoding, State};

use super::{
    cenc::{generic_hash, generic_hash_with_key, validate_id, ReplyMsgData, RequestMsgData},
    thirty_two_random_bytes, Addr, Command,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Reply {
    pub tid: u16,
    pub rtt: usize,
    pub from: Addr,
    pub to: Addr,
    pub token: Option<[u8; 32]>,
    pub closer_nodes: Vec<Addr>,
    pub error: usize,
    pub value: Option<Vec<u8>>,
}

impl Reply {
    pub fn decode(from: &Addr, buff: &[u8], state: &mut State) -> Result<Self> {
        let data = ReplyMsgData::decode(buff, state)?;
        Self::from_data(from, data)
    }
    fn from_data(from: &Addr, data: ReplyMsgData) -> Result<Self> {
        let mut new_from = from.clone();
        if let Some(id) = data.id {
            if let Some(valid_id) = validate_id(&id, from) {
                new_from.id = valid_id.to_vec().into();
            }
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
    /// in js we use table_id if :
    /// `const id = this._io.ephemeral === false && socket === this._io.serverSocket`
    /// token is io.secrets(Req.from, 1)
    /// closer_nodes is req.target && io.table.closes(req.target)
    //fn encode(
    //    &self,
    //    _table_id: Option<[u8; 32]>,
    //    _error: u8,
    //    _token: Option<[u8; 32]>,
    //    _closer_nodes: Vec<Addr>,
    //    _destination: Addr,
    //) -> Result<Vec<u8>> {
    fn encode(
        &self,
        table_id: &[u8; 32],
        ephemeral: bool,
        is_server_socket: bool,
    ) -> Result<Vec<u8>> {
        let id = !ephemeral && is_server_socket;
        let mut state = State::new();
        // (type | version) + flags + to + tid
        state.add_end(1 + 1 + 6 + 2)?;
        if id {
            state.add_end(32)?;
        }
        if self.token.is_some() {
            state.add_end(32)?;
        }
        if !self.closer_nodes.is_empty() {
            state.preencode_usize_var(&self.closer_nodes.len())?;
            for n in &self.closer_nodes {
                state.preencode(n)?;
            }
        }
        if self.error > 0 {
            state.preencode(&self.error)?;
        }
        if let Some(v) = &self.value {
            state.preencode(v)?;
        }

        let mut buff = state.create_buffer();
        buff[state.start()] = RESPONSE_ID;
        state.add_start(1)?;

        let mut flags: u8 = 0;
        if id {
            flags |= 1 << 0;
        }
        if self.token.is_some() {
            flags |= 1 << 1;
        }
        if !self.closer_nodes.is_empty() {
            flags |= 1 << 2;
        }
        if self.error > 0 {
            flags |= 1 << 3;
        }
        if self.value.is_some() {
            flags |= 1 << 4;
        }
        buff[state.start()] = flags;

        state.encode_u16(self.tid, &mut buff)?;
        state.encode(&self.from, &mut buff)?;

        if id {
            state.encode_fixed_32(table_id, &mut buff)?;
        }
        if let Some(t) = self.token {
            state.encode_fixed_32(&t, &mut buff)?;
        }
        if !self.closer_nodes.is_empty() {
            state.encode_usize_var(&self.closer_nodes.len(), &mut buff)?;
            for n in &self.closer_nodes {
                state.encode(n, &mut buff)?;
            }
        }
        if self.error > 0 {
            state.encode(&self.error, &mut buff)?;
        }
        if let Some(v) = &self.value {
            state.encode_fixed_32(v, &mut buff)?;
        }
        Ok(buff.into())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub tid: u16,
    pub from: Option<Addr>,
    // TODO remove this field?
    pub to: Option<Addr>,
    pub token: Option<[u8; 32]>,
    pub internal: bool,
    pub command: Command,
    pub target: Option<[u8; 32]>,
    pub value: Option<Vec<u8>>,
}

impl Request {
    pub fn decode(from: &Addr, buff: &[u8], state: &mut State) -> Result<Self> {
        let data = RequestMsgData::decode(buff, state);
        Request::from_data(data?, from)
    }
    fn from_data(data: RequestMsgData, from: &Addr) -> Result<Self> {
        let mut new_from = from.clone();
        if let Some(id) = data.id {
            if let Some(valid_id) = validate_id(&id, from) {
                new_from.id = valid_id.to_vec().into();
            }
        }
        Ok(Request {
            tid: data.tid,
            from: Some(new_from),
            to: Some(data.to),
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
    pub fn encode(&self, io: &Io, is_server_socket: bool) -> Result<Vec<u8>> {
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
                .unwrap_or("null".to_string()),
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

/// Io recieves data events to and uses it, and it's own data to construct [`Message`]
pub enum MessageEvents {
    Ping,
    FindNode { target: [u8; 32] },
    PingNat { value: Vec<u8> },
    DownHint { value: Vec<u8> },
    // add stuff for custom commands later
    // Reply { error, value, token, hase_closer_nodes },
}

#[derive(Debug, Clone)]
pub enum Message {
    Request(Request),
    Reply(Reply),
}

#[derive(Debug)]
pub struct Io {
    tid: AtomicU16,
    ephemeral: bool,
    socket: Arc<TRwLock<UdxSocket>>,
    inflight: Inflight,
}

impl Io {
    pub fn new() -> Result<Self> {
        let socket = Arc::new(TRwLock::new(UdxSocket::bind("0.0.0.0:0")?));
        let recv_socket = socket.clone();
        let inflight: Inflight = Default::default();
        let recv_inflight = inflight.clone();
        tokio::spawn(async move {
            loop {
                // TODO add timeout so rw is not locked forever
                let x = recv_socket.read().await.recv().await;
                if let Ok((addr, buff)) = x {
                    if let Ok(message) = decode_message(buff, addr) {
                        match message {
                            Message::Reply(reply) => {
                                // Handle reply with existing inflight logic
                                if let Some((sender, _)) =
                                    recv_inflight.write().unwrap().remove(&reply.tid)
                                {
                                    let _ = sender.send(reply);
                                }
                            }
                            Message::Request(_) => {} // Requests handled via the Stream
                        }
                    }
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
    //this.dht._request(node, true,     DOWN_HINT, null,   state.buffer, this._session, noop,       noop)
    //_request (          to,   internal, command, target, value,        session,       onresponse, onerror) {
    pub fn create_down_hint(&self, to: &Addr, value: Vec<u8>) -> Request {
        self.create_request(to, None, true, Command::DownHint, None, Some(value))
    }

    pub async fn send_find_node(&self, to: &Addr, target: &[u8; 32]) -> Result<Receiver<Reply>> {
        let req = self.create_find_node(to, target);
        self.send(req).await
    }
    pub async fn send_ping(&self, to: &Addr) -> Result<Receiver<Reply>> {
        let req = self.create_ping(to);
        self.send(req).await
    }

    pub async fn send(&self, request: Request) -> Result<Receiver<Reply>> {
        let buff = request.encode(self, false)?;
        let socket_addr = SocketAddr::from(
            request
                .to
                .as_ref()
                .ok_or_else(|| crate::Error::RequestRequiresToField)?,
        );
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
        REQUEST_ID => Message::Request(Request::decode(&from, &buff, &mut state)?),
        RESPONSE_ID => Message::Reply(Reply::decode(&from, &buff, &mut state)?),
        _ => todo!("eror"),
    })
}
struct Secrets {
    rotate_secrets: usize,
    // NB starts null in js. Not initialized until token call.
    // so my behavior diverges when drain called until token
    // bc drain checks if secrets initialized
    secrets: [[u8; 32]; 2],
}

impl Secrets {
    fn new() -> Self {
        Self {
            rotate_secrets: 10,
            secrets: [thirty_two_random_bytes(), thirty_two_random_bytes()],
        }
    }

    fn drain(&mut self) -> Result<()> {
        self.rotate_secrets -= 1;
        if self.rotate_secrets == 0 {
            self.rotate_secrets = 10;
            let tmp = self.secrets[0];
            self.secrets[0] = self.secrets[1];
            self.secrets[1] = generic_hash(&tmp)?;
        }
        Ok(())
    }

    fn token(&self, addr: &Addr, secret_index: usize) -> Result<[u8; 32]> {
        generic_hash_with_key(&addr.host.octets(), &self.secrets[secret_index])
    }
}
