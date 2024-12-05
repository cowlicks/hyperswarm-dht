pub(crate) const HASH_SIZE: usize = 32;
pub(crate) const ID_SIZE: usize = 32;
pub(crate) const VERSION: u8 = 0b11;
pub(crate) const RESPONSE_ID: u8 = (0b0001 << 4) | VERSION;
pub(crate) const REQUEST_ID: u8 = 0b0000 << 4 | VERSION;
// From dht-rpc/index.js tick interval for the rpc client
pub(crate) const TICK_INTERVAL_MS: u64 = 5000;
