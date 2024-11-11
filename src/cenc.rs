use std::{convert::TryFrom, net::Ipv4Addr};

use compact_encoding::{CompactEncoding, EncodingError, State};

use crate::{
    constants::{HASH_SIZE, ID_SIZE},
    udx::{Addr, Command},
    Error, Result,
};

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
