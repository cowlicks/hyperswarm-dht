//! udx/dht-rpc internnals
//!
#![allow(unreachable_code, dead_code)]
use std::cell::RefCell;
use std::convert::{TryFrom, TryInto};
use std::fmt::Display;
use std::future::IntoFuture;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;

use crate::kbucket::{Entry, KBucketsTable, KeyBytes};
use crate::{udx::io::Io, Result};
use io::Reply;
use log::debug;
use rand::{
    rngs::{OsRng, StdRng},
    RngCore, SeedableRng,
};

mod cenc;
mod io;
mod sio;
mod stream;

#[cfg(test)]
mod test;

/// TODO in js this is de/encoded with c.uint which is for a variable sized unsigned integer.
// but it is always one byte. We use a u8 instead of a usize here. So there is a limit on 256
// commands.
#[derive(Copy, Debug, Clone, PartialEq)]
#[repr(u8)]
pub enum Command {
    Ping = 0,
    PingNat,
    FindNode,
    DownHint,
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Command::*;
        write!(f, "COMMAND")?;
        match self {
            Ping => write!(f, "::Ping"),
            PingNat => write!(f, "::PingNat"),
            FindNode => write!(f, "::FindNode"),
            DownHint => write!(f, "::DownHint"),
        }
    }
}

impl TryFrom<u8> for Command {
    type Error = crate::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use Command::*;
        Ok(match value {
            0 => Ping,
            1 => PingNat,
            2 => FindNode,
            3 => DownHint,
            x => return Err(crate::Error::InvalidRpcCommand(x)),
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
// TODO rename to Peer
pub struct Addr {
    // TODO change ot [u8; 32]
    pub id: Option<[u8; 32]>,
    pub host: Ipv4Addr,
    pub port: u16,
}

impl From<&Addr> for SocketAddr {
    fn from(value: &Addr) -> Self {
        SocketAddr::V4(SocketAddrV4::new(value.host, value.port))
    }
}

impl From<&SocketAddr> for Addr {
    fn from(value: &SocketAddr) -> Self {
        let SocketAddr::V4(s) = value else { panic!() };
        Addr {
            id: None,
            host: *s.ip(),
            port: s.port(),
        }
    }
}

#[derive(Debug, derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct RpcDht {
    // TODO this should be a `Cell` since it is only mutated from  one place (this struct).
    #[builder(default = "Arc::new(RefCell::new(thirty_two_random_bytes()))")]
    pub id: Arc<RefCell<[u8; 32]>>,
    #[builder(field(ty = "Vec<SocketAddr>"))]
    bootstrap_nodes: Vec<SocketAddr>,
    io: Io,
    // TODO make this automatically set using the id
    kbuckets: KBucketsTable<KeyBytes, Addr>,
}

impl RpcDhtBuilder {
    fn add_bootstrap_node(
        mut self,
        addr: SocketAddr,
    ) -> std::result::Result<Self, RpcDhtBuilderError> {
        self.bootstrap_nodes.push(addr);
        Ok(self)
    }
    fn with_default_io(mut self) -> Result<Self> {
        if let None = self.id {
            self.id = Some(Arc::new(RefCell::new(thirty_two_random_bytes())));
        }
        let id = match self.id {
            None => Arc::new(RefCell::new(thirty_two_random_bytes())),
            Some(id) => id,
        };
        self.io = Some(Io::new(id.clone())?);
        self.id = Some(id);
        Ok(self)
    }
}

impl From<[u8; 32]> for KeyBytes {
    fn from(value: [u8; 32]) -> Self {
        Self::new(value)
    }
}

impl RpcDht {
    async fn bootstrap(&mut self) -> Result<Reply> {
        let Some(node_addr) = self.bootstrap_nodes.last() else {
            panic!()
        };
        let to = Addr::from(node_addr);
        let receiver = self.io.send_find_node(&to, &self.id.borrow()).await?;
        let reply = receiver.into_future().await?;
        self.add_node(&reply.from)?;
        Ok(reply)
    }

    fn add_node(&mut self, peer: &Addr) -> Result<()> {
        use crate::kbucket::InsertResult::*;
        let Some(id) = peer.id.clone() else {
            todo!("No peer.id: {peer:?}")
        };
        let kb: [u8; 32] = id.clone().try_into().unwrap();
        let kb: KeyBytes = kb.into();
        if let Entry::Absent(entry) = self.kbuckets.entry(&kb) {
            match entry.insert(peer.clone(), crate::kbucket::NodeStatus::Connected) {
                Inserted => debug!("Inserted [id = {id:?}] into kbuckets"),
                Pending { .. } => debug!("Pending? [id = {id:?}]"),
                Full => debug!("Bucket full [id = {id:?}] not added to kbuckets"),
            }
        } else {
            todo!()
        }
        Ok(())
    }

    async fn ping(&self, addr: &SocketAddr) -> Result<Reply> {
        let to = Addr::from(addr);
        let receiver = self.io.send_find_node(&to, &self.id.borrow()).await?;
        Ok(receiver.into_future().await?)
    }

    fn query(&mut self, _command: Command, _id: [u8; 32], _value: Option<Vec<u8>>) {
        todo!()
    }
}

fn thirty_two_random_bytes() -> [u8; 32] {
    let mut buff = [0; 32];
    let mut rng = StdRng::from_rng(OsRng).unwrap();
    rng.fill_bytes(&mut buff);
    buff
}
