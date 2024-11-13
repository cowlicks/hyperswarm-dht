use std::{
    convert::{TryFrom, TryInto},
    net::Ipv4Addr,
};

use compact_encoding::{CompactEncoding, EncodingError, State};

use crate::{
    constants::{HASH_SIZE, ID_SIZE},
    udx::{Addr, Command},
    Error, Result,
};

use super::io::{Reply, Request};

impl CompactEncoding<Command> for State {
    fn preencode(&mut self, _value: &Command) -> std::result::Result<usize, EncodingError> {
        Ok(self.add_end(1)?)
    }

    fn encode(
        &mut self,
        value: &Command,
        buff: &mut [u8],
    ) -> std::result::Result<usize, EncodingError> {
        buff[self.start()] = value.clone() as u8;
        self.add_start(1)?;
        Ok(self.start())
    }

    fn decode(&mut self, buff: &[u8]) -> std::result::Result<Command, EncodingError> {
        let cmd = Command::try_from(buff[self.start()]).map_err(|e| e.into())?;
        self.add_start(1)?;
        Ok(cmd)
    }
}

impl Into<EncodingError> for Error {
    fn into(self) -> EncodingError {
        EncodingError {
            kind: compact_encoding::EncodingErrorKind::InvalidData,
            message: self.to_string(),
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

impl CompactEncoding<Addr> for State {
    fn preencode(&mut self, _value: &Addr) -> std::result::Result<usize, EncodingError> {
        Ok(self.add_end(6)?)
    }

    fn encode(
        &mut self,
        value: &Addr,
        buff: &mut [u8],
    ) -> std::result::Result<usize, EncodingError> {
        encode_ip(&value.host, buff, self)?;
        self.encode_u16(value.port, buff)?;
        Ok(self.start())
    }

    fn decode(&mut self, buff: &[u8]) -> std::result::Result<Addr, EncodingError> {
        let ip_start = self.start();
        let [ip1, ip2, ip3, ip4, port_1, port_2] = buff[ip_start..(ip_start + 4 + 2)] else {
            todo!()
        };
        self.add_start(4 + 2)?;
        let host = Ipv4Addr::from([ip1, ip2, ip3, ip4]);
        let port = u16::from_le_bytes([port_1, port_2]);
        Ok(Addr {
            id: None,
            host,
            port,
        })
    }
}

pub(crate) fn calculate_id(from: &Addr) -> Result<[u8; ID_SIZE]> {
    let mut from_buff = vec![0; 6];
    let mut state = State::new_with_start_and_end(0, 6);
    encode_ip(&from.host, &mut from_buff, &mut state)?;
    state.encode_u16(from.port, &mut from_buff)?;
    generic_hash(&from_buff)
}

fn generic_hash(input: &[u8]) -> Result<[u8; HASH_SIZE]> {
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

pub fn decode_request(buff: &[u8], mut from: Addr, state: &mut State) -> Result<Request> {
    let flags = buff[state.start()];
    state.add_start(1)?;

    let tid = state.decode_u16(buff)?;

    let to = Some(state.decode(buff)?);

    let id = decode_fixed_32_flag(flags, 1, state, buff)?;
    let token = decode_fixed_32_flag(flags, 2, state, buff)?;

    let internal = (flags & 4) != 0;
    let command: Command = state.decode(buff)?;

    let target = decode_fixed_32_flag(flags, 8, state, buff)?;

    let value = if flags & 16 > 0 {
        Some(state.decode_buffer(buff)?.as_ref().to_vec())
    } else {
        None
    };

    if let Some(id) = id {
        if let Some(valid_id) = validate_id(&id, &from) {
            from.id = valid_id.to_vec().into();
        }
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

fn validate_id(id: &[u8; ID_SIZE], from: &Addr) -> Option<[u8; ID_SIZE]> {
    if let Ok(result) = calculate_id(from) {
        if *id == result {
            return Some(*id);
        }
    }
    None
}

fn decode_fixed_32_flag(
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
    return Ok(None);
}
/// Decode an u32 array
pub fn decode_addr_array(state: &mut State, buffer: &[u8]) -> Result<Vec<Addr>> {
    let len = state.decode_usize_var(buffer)?;
    let mut value: Vec<Addr> = Vec::with_capacity(len);
    for _ in 0..len {
        let add: Addr = state.decode(buffer)?;
        value.push(add);
    }
    Ok(value)
}

pub fn decode_reply(buff: &[u8], mut from: Addr, state: &mut State) -> Result<Reply> {
    let flags = buff[state.start()];
    state.add_start(1)?;

    let tid = state.decode_u16(buff)?;
    let to: Addr = state.decode(buff)?;

    let id = decode_fixed_32_flag(flags, 1, state, buff)?;
    let token = decode_fixed_32_flag(flags, 2, state, buff)?;

    let closer_nodes: Option<Vec<Addr>> = if flags & 4 > 0 {
        Some(decode_addr_array(state, buff)?)
    } else {
        None
    };

    let error: u8 = if flags & 8 > 0 {
        state.decode_u8(buff)?
    } else {
        0
    };

    let value = if flags & 16 > 0 {
        Some(state.decode_buffer(buff)?.to_vec())
    } else {
        None
    };

    if let Some(id) = id {
        if let Some(valid_id) = validate_id(&id, &from) {
            from.id = valid_id.to_vec().into();
        }
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
