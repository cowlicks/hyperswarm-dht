use std::{
    borrow::Borrow,
    convert::{TryFrom, TryInto},
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use compact_encoding::{CompactEncoding, EncodingError, EncodingErrorKind, State};

use crate::{
    constants::{HASH_SIZE, ID_SIZE, REQUEST_ID, RESPONSE_ID},
    kbucket::{self, EntryView},
    peers::PeersEncoding,
    udx::InternalCommand,
    Error, IdBytes, PeerId, Result,
};

use super::{
    io::{Reply, Request},
    message::{MsgData, ReplyMsgData, RequestMsgData},
    smod::{Node, Peer},
    Command, ExternalCommand,
};

impl CompactEncoding<InternalCommand> for State {
    fn preencode(&mut self, _value: &InternalCommand) -> std::result::Result<usize, EncodingError> {
        self.add_end(1)
    }

    fn encode(
        &mut self,
        value: &InternalCommand,
        buff: &mut [u8],
    ) -> std::result::Result<usize, EncodingError> {
        buff[self.start()] = *value as u8;
        self.add_start(1)?;
        Ok(self.start())
    }

    fn decode(&mut self, buff: &[u8]) -> std::result::Result<InternalCommand, EncodingError> {
        let cmd = InternalCommand::try_from(buff[self.start()]).map_err(EncodingError::from)?;
        self.add_start(1)?;
        Ok(cmd)
    }
}

impl From<Error> for EncodingError {
    fn from(value: Error) -> Self {
        EncodingError {
            kind: compact_encoding::EncodingErrorKind::InvalidData,
            message: value.to_string(),
        }
    }
}

fn encode_ip(
    ip: &Ipv4Addr,
    buff: &mut [u8],
    state: &mut State,
) -> std::result::Result<(), EncodingError> {
    let ip_bytes = ip.octets();
    buff[state.start()] = ip_bytes[0];
    state.add_start(1)?;
    buff[state.start()] = ip_bytes[1];
    state.add_start(1)?;
    buff[state.start()] = ip_bytes[2];
    state.add_start(1)?;
    buff[state.start()] = ip_bytes[3];
    state.add_start(1)?;
    Ok(())
}

impl CompactEncoding<Peer> for State {
    fn preencode(&mut self, _value: &Peer) -> std::result::Result<usize, EncodingError> {
        self.add_end(6)
    }

    fn encode(
        &mut self,
        value: &Peer,
        buff: &mut [u8],
    ) -> std::result::Result<usize, EncodingError> {
        if let IpAddr::V4(ip) = value.addr.ip() {
            encode_ip(&ip, buff, self)?;
            self.encode_u16(value.addr.port(), buff)?;
            return Ok(self.start());
        }
        Err(EncodingError {
            kind: EncodingErrorKind::InvalidData,
            message: "ipv6 not supported".to_string(),
        })
    }

    fn decode(&mut self, buff: &[u8]) -> std::result::Result<Peer, EncodingError> {
        let ip_start = self.start();
        let [ip1, ip2, ip3, ip4, port_1, port_2] = buff[ip_start..(ip_start + 4 + 2)] else {
            todo!()
        };
        self.add_start(4 + 2)?;
        let host = Ipv4Addr::from([ip1, ip2, ip3, ip4]);
        let port = u16::from_le_bytes([port_1, port_2]);
        Ok(Peer {
            id: None,
            addr: SocketAddr::from((host, port)),
            referrer: None,
        })
    }
}

pub fn ipv4(addr: &SocketAddr) -> Result<Ipv4Addr> {
    if let IpAddr::V4(ip) = addr.ip() {
        return Ok(ip);
    }
    Err(crate::Error::Ipv6NotSupported)
}

const IP_AND_PORT_NUM_BYTES: usize = 6;

/// TODO this will panic for ipv6
fn id_from_socket(addr: &SocketAddr) -> [u8; ID_SIZE] {
    let mut from_buff = vec![0; IP_AND_PORT_NUM_BYTES];
    let mut state = State::new_with_start_and_end(0, IP_AND_PORT_NUM_BYTES);
    let ip = ipv4(addr).expect("TODO what to do about ipv6");
    encode_ip(&ip, &mut from_buff, &mut state).expect("IP_AND_PORT_NUM_BYTES is correct");
    state
        .encode_u16(addr.port(), &mut from_buff)
        .expect("IP_AND_PORT_NUM_BYTES is correct");
    generic_hash(&from_buff).expect("checked correct")
}

pub(crate) fn calculate_peer_id(from: &Peer) -> [u8; ID_SIZE] {
    id_from_socket(&from.addr)
}

pub(crate) fn generic_hash(input: &[u8]) -> Result<[u8; HASH_SIZE]> {
    let mut out = [0; HASH_SIZE];
    let ret = unsafe {
        libsodium_sys::crypto_generichash(
            out.as_mut_ptr(),
            out.len(),
            input.as_ptr(),
            input.len() as u64,
            std::ptr::null(),
            0,
        )
    };
    if ret != 0 {
        return Err(Error::LibSodiumGenericHashError(ret));
    }
    Ok(out)
}

pub(crate) fn generic_hash_with_key(input: &[u8], key: &[u8]) -> Result<[u8; HASH_SIZE]> {
    let mut out = [0; HASH_SIZE];
    let ret = unsafe {
        libsodium_sys::crypto_generichash(
            out.as_mut_ptr(),
            out.len(),
            input.as_ptr(),
            input.len() as u64,
            key.as_ptr(),
            key.len(),
        )
    };
    if ret != 0 {
        return Err(Error::LibSodiumGenericHashError(ret));
    }
    Ok(out)
}

pub fn decode_request(buff: &[u8], mut from: Peer, state: &mut State) -> Result<Request> {
    let flags = buff[state.start()];
    state.add_start(1)?;

    let tid = state.decode_u16(buff)?;

    let to = state.decode(buff)?;

    let id = decode_fixed_32_flag(flags, 1, state, buff)?;
    let token = decode_fixed_32_flag(flags, 2, state, buff)?;

    let internal = (flags & 4) != 0;
    let command = if internal {
        let cmd: InternalCommand = state.decode(buff)?;
        Command::Internal(cmd)
    } else {
        let cmd: u8 = state.decode(buff)?;
        Command::External(ExternalCommand(cmd as usize))
    };

    let target = decode_fixed_32_flag(flags, 8, state, buff)?;

    let value = if flags & 16 > 0 {
        Some(state.decode_buffer(buff)?.as_ref().to_vec())
    } else {
        None
    };

    if let Some(id) = id {
        from.id = validate_id(&id, &from);
    }

    Ok(Request {
        tid,
        from: Some(from),
        to,
        token,
        internal,
        command,
        target,
        value,
    })
}

pub(crate) fn validate_id(id: &[u8; ID_SIZE], from: &Peer) -> Option<[u8; ID_SIZE]> {
    if id == &calculate_peer_id(from) {
        return Some(*id);
    }
    None
}

pub(crate) fn decode_fixed_32_flag(
    flags: u8,
    shift: u8,
    state: &mut State,
    buff: &[u8],
) -> Result<Option<[u8; 32]>> {
    if flags & shift > 0 {
        return Ok(Some(
            state.decode_fixed_32(buff)?.as_ref().try_into().unwrap(),
        ));
    }
    Ok(None)
}
/// Decode an u32 array
pub fn decode_addr_array(state: &mut State, buffer: &[u8]) -> Result<Vec<Peer>> {
    let len = state.decode_usize_var(buffer)?;
    let mut value: Vec<Peer> = Vec::with_capacity(len);
    for _ in 0..len {
        let add: Peer = state.decode(buffer)?;
        value.push(add);
    }
    Ok(value)
}

pub fn decode_reply(buff: &[u8], mut from: Peer, state: &mut State) -> Result<Reply> {
    let flags = buff[state.start()];
    state.add_start(1)?;

    let tid = state.decode_u16(buff)?;
    let to: Peer = state.decode(buff)?;

    let id = decode_fixed_32_flag(flags, 1, state, buff)?;
    let token = decode_fixed_32_flag(flags, 2, state, buff)?;

    let closer_nodes: Vec<Peer> = if flags & 4 > 0 {
        decode_addr_array(state, buff)?
    } else {
        vec![]
    };

    let error = if flags & 8 > 0 {
        state.decode_usize_var(buff)?
    } else {
        0
    };

    let value = if flags & 16 > 0 {
        Some(state.decode_buffer(buff)?.to_vec())
    } else {
        None
    };

    if let Some(id) = id {
        from.id = validate_id(&id, &from);
    }
    Ok(Reply {
        tid,
        rtt: 0,
        from,
        to,
        token,
        closer_nodes,
        error,
        value,
    })
}
impl RequestMsgData {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut state = State::new();
        state.add_end(1 + 1 + 6 + 2)?;

        if self.id.is_some() {
            state.add_end(ID_SIZE)?;
        }
        if self.token.is_some() {
            state.add_end(32)?;
        }

        //let cmd = self.command.clone() as usize;
        //state.preencode_usize_var(&self.command)?;
        // One byte for command as u8
        state.add_end(1)?;

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
        if self.id.is_some() {
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

        state.encode(&self.to, &mut buff)?;

        if let Some(id) = &self.id {
            state.encode_fixed_32(id, &mut buff)?;
        }
        if let Some(t) = self.token {
            // c.fixed32.encode(state, token)
            state.encode_fixed_32(&t, &mut buff)?;
        }

        buff[state.start()] = self.command.encode();
        state.add_start(1)?;

        // c.uint.encode(state, this.command)
        if let Some(t) = &self.target {
            // c.fixed32.encode(state, this.target)
            state.encode_fixed_32(t, &mut buff)?;
        }
        if let Some(v) = &self.value {
            state.encode_buffer(v, &mut buff)?;
        }
        Ok(buff.to_vec())
    }
    pub fn decode(buff: &[u8], state: &mut State) -> Result<RequestMsgData> {
        let flags = buff[state.start()];
        state.add_start(1)?;

        let tid = state.decode_u16(buff)?;

        let to = state.decode(buff)?;

        let id = decode_fixed_32_flag(flags, 1, state, buff)?;
        let token = decode_fixed_32_flag(flags, 2, state, buff)?;

        let internal = (flags & 4) != 0;
        let command = if internal {
            let cmd: InternalCommand = state.decode(buff)?;
            Command::Internal(cmd)
        } else {
            let cmd: u8 = state.decode(buff)?;
            Command::External(ExternalCommand(cmd as usize))
        };

        let target = decode_fixed_32_flag(flags, 8, state, buff)?;

        let value = if flags & 16 > 0 {
            Some(state.decode_buffer(buff)?.as_ref().to_vec())
        } else {
            None
        };

        Ok(RequestMsgData {
            tid,
            to,
            id,
            internal,
            token,
            command,
            target,
            value,
        })
    }
}

impl ReplyMsgData {
    pub fn is_error(&self) -> bool {
        self.error != 0
    }
    /// Decode the `to` field into `PeerId`
    pub(crate) fn decode_closer_nodes(&self) -> Vec<PeerId> {
        self.closer_nodes
            .iter()
            .filter_map(|p| {
                if let Some(id) = p.id {
                    return Some(PeerId::new(p.addr, IdBytes::from(id)));
                }
                None
            })
            .collect()
    }
    pub(crate) fn valid_id_bytes(&self) -> Option<IdBytes> {
        self.id.map(IdBytes::from)
    }

    fn encode(&self) -> Result<Vec<u8>> {
        let mut state = State::new();
        // (type | version) + flags + to + tid
        state.add_end(1 + 1 + 6 + 2)?;
        if self.id.is_some() {
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

        if self.id.is_some() {
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
        state.add_start(1)?;

        state.encode_u16(self.tid, &mut buff)?;
        state.encode(&self.to, &mut buff)?;

        if let Some(id) = &self.id {
            state.encode_fixed_32(id, &mut buff)?;
        }
        if let Some(token) = self.token {
            state.encode_fixed_32(&token, &mut buff)?;
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
        if let Some(value) = &self.value {
            state.encode_fixed_32(value, &mut buff)?;
        }
        Ok(buff.into())
    }

    pub fn decode(buff: &[u8], state: &mut State) -> Result<ReplyMsgData> {
        let flags = buff[state.start()];
        state.add_start(1)?;

        let tid = state.decode_u16(buff)?;
        let to: Peer = state.decode(buff)?;

        let id = decode_fixed_32_flag(flags, 1, state, buff)?;
        let token = decode_fixed_32_flag(flags, 2, state, buff)?;

        let closer_nodes: Vec<Peer> = if flags & 4 > 0 {
            decode_addr_array(state, buff)?
        } else {
            vec![]
        };

        let error = if flags & 8 > 0 {
            state.decode_usize_var(buff)?
        } else {
            0
        };

        let value = if flags & 16 > 0 {
            Some(state.decode_buffer(buff)?.to_vec())
        } else {
            None
        };
        Ok(ReplyMsgData {
            tid,
            to,
            id,
            token,
            closer_nodes,
            error,
            value,
        })
    }
}

impl MsgData {
    pub fn encode(&self) -> Result<Vec<u8>> {
        match self {
            MsgData::Request(request) => request.encode(),
            MsgData::Reply(reply) => reply.encode(),
        }
    }
    pub fn decode(buff: &[u8]) -> Result<MsgData> {
        let mut state = State::new_with_start_and_end(1, buff.len());
        Ok(match buff[0] {
            REQUEST_ID => MsgData::Request(RequestMsgData::decode(buff, &mut state)?),
            RESPONSE_ID => MsgData::Reply(ReplyMsgData::decode(buff, &mut state)?),
            _ => todo!(),
        })
    }
}

impl PeersEncoding for &SocketAddr {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(6);
        // TODO what to do with IPV6?
        if let IpAddr::V4(ip) = self.ip() {
            buf.extend_from_slice(&ip.octets()[..]);
            buf.extend_from_slice(&self.port().to_be_bytes()[..]);
        }
        buf
    }
}

impl PeersEncoding for Vec<EntryView<kbucket::Key<IdBytes>, Node>> {
    fn encode(&self) -> Vec<u8> {
        // TODO refactor
        let mut buf = Vec::with_capacity(self.len() * (32 + 6));

        for peer in self.iter() {
            let addr = &peer.node.value.addr;
            if let IpAddr::V4(ip) = addr.ip() {
                buf.extend_from_slice(peer.node.key.preimage().borrow());
                buf.extend_from_slice(&ip.octets()[..]);
                buf.extend_from_slice(&addr.port().to_be_bytes()[..]);
            }
        }
        buf
    }
}
