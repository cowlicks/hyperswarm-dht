//! Rust Implementation of the hyperswarm DHT
#![warn(rust_2018_idioms)]
#![allow(unreachable_code)]

use std::{array::TryFromSliceError, net::AddrParseError};

use compact_encoding::EncodingError;
use rpc::RpcDhtBuilderError;
use tokio::sync::oneshot::error::RecvError;

pub use crate::rpc::{fill_random_bytes, DhtConfig, IdBytes, Peer, PeerId};

#[cfg(test)]
pub mod test;

pub mod constants;
pub mod kbucket;
pub mod peers;
pub mod rpc;
mod util;

/// The publicly available hyperswarm DHT addresses
pub const DEFAULT_BOOTSTRAP: [&str; 3] = [
    "node1.hyperdht.org:49737",
    "node2.hyperdht.org:49737",
    "node3.hyperdht.org:49737",
];

/// The command identifier for `Mutable` storage
pub const MUTABLE_STORE_CMD: usize = 1;
/// The command identifier for immutable storage
pub const IMMUTABLE_STORE_CMD: usize = 2;
/// The command identifier to (un)announce/lookup peers
pub const PEERS_CMD: usize = 3;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Error from compact_encoding: {0}")]
    CompactEncodingError(EncodingError),
    #[error("IO Eror")]
    IoError(#[from] std::io::Error),
    #[error("Invalid RPC command in message: {0}")]
    InvalidRpcCommand(u8),
    #[error("Incorrect message ID size. Expected 32. Error: {0}")]
    IncorrectMessageIdSize(TryFromSliceError),
    #[error("Error in libsodium's genric_hash function. Return value: {0}")]
    LibSodiumGenericHashError(i32),
    #[error("RpcDhtBuilderError: {0}")]
    RpcDhtBuilderError(#[from] RpcDhtBuilderError),
    #[error("RecvError: {0}")]
    RecvError(#[from] RecvError),
    #[error("AddrParseError: {0}")]
    AddrParseError(#[from] AddrParseError),
    #[error("Requests must have a 'to' field")]
    RequestRequiresToField,
    #[error("Ipv6 not supported")]
    Ipv6NotSupported,
}

pub type Result<T> = std::result::Result<T, Error>;

/// TODO make EncodingError impl Error trait
impl From<EncodingError> for Error {
    fn from(value: EncodingError) -> Self {
        Error::CompactEncodingError(value)
    }
}
