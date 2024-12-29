use blake2::{
    crypto_mac::generic_array::{typenum::U64, GenericArray},
    Blake2b, VarBlake2b,
};
use ed25519_dalek::SignatureError;
pub use ed25519_dalek::{ExpandedSecretKey, Keypair, PublicKey, SecretKey, Signature, Verifier};

use crate::dht_proto::Mutable;
use ::dht_rpc::{fill_random_bytes, IdBytes};

/// VALUE_MAX_SIZE + packet overhead (i.e. the key etc.)
/// should be less than the network MTU, normally 1400 bytes
pub const VALUE_MAX_SIZE: usize = 1000;

const SALT_SEG: &[u8; 6] = b"4:salt";
const SEQ_SEG: &[u8; 6] = b"3:seqi";
const V_SEG: &[u8; 3] = b"1:v";

/// Utility method for creating a random or hashed salt value.
///
/// # Panics
///
/// Size must be in range [16-64], panics otherwise
#[inline]
pub fn salt(val: &[u8], size: usize) -> Vec<u8> {
    use blake2::digest::{Update, VariableOutput};
    assert!((16..=64).contains(&size));
    let mut salt = Vec::with_capacity(size);
    let mut hasher = VarBlake2b::new(size).unwrap();
    hasher.update(val);
    hasher.finalize_variable(|res| salt.extend_from_slice(res));
    salt
}

/// Fill a new `Vec` with `size` random bytes
///
/// # Panics
///
/// Size must be in range [16-64], panics otherwise
#[inline]
pub fn random_salt(size: usize) -> Vec<u8> {
    assert!((16..=64).contains(&size));
    let mut salt = vec![0; size];
    fill_random_bytes(&mut salt);
    salt
}

/// Sign the value as [`signable`] using the keypair.
#[inline]
pub fn sign(
    public_key: &PublicKey,
    secret: &SecretKey,
    value: &[u8],
    salt: Option<&Vec<u8>>,
    seq: u64,
) -> Signature {
    let msg = signable(value, salt, seq).expect("salt exceeds max len");
    ExpandedSecretKey::from(secret).sign(&msg, public_key)
}

/// Verify a signature on a message with a keypair's public key.
#[inline]
pub fn verify(public: &PublicKey, msg: &[u8], sig: &Signature) -> Result<(), SignatureError> {
    public.verify(msg, sig)
}

/// Create a 64B `blake2b` hash of `val`.
#[inline]
pub fn hash(val: &[u8]) -> GenericArray<u8, U64> {
    use blake2::Digest;
    let mut hasher = Blake2b::new();
    hasher.update(val);
    hasher.finalize()
}

/// hash the `val` with a key size of U32 and put it into [`IdBytes`]
pub fn hash_id(val: &[u8]) -> IdBytes {
    use blake2::digest::{Update, VariableOutput};
    let mut key = [0; 32];
    let mut hasher = VarBlake2b::new(32).unwrap();
    hasher.update(val);
    hasher.finalize_variable(|res| key.copy_from_slice(res));
    key.into()
}

/// Generate a new `Ed25519` key pair.
#[inline]
pub fn keypair() -> Keypair {
    use rand::{
        rngs::{OsRng, StdRng},
        SeedableRng,
    };
    Keypair::generate(&mut StdRng::from_rng(OsRng).unwrap())
}

#[inline]
pub fn signature(mutable: &Mutable) -> Option<Signature> {
    if let Some(ref sig) = mutable.signature {
        Signature::from_bytes(sig).ok()
    } else {
        None
    }
}

#[allow(clippy::result_unit_err)]
pub fn signable(value: &[u8], salt: Option<&Vec<u8>>, seq: u64) -> Result<Vec<u8>, ()> {
    let cap = SEQ_SEG.len() + 3 + V_SEG.len() + 3 + value.len();

    let mut s = if let Some(salt) = salt {
        if salt.len() > 64 {
            return Err(());
        }
        let mut s = Vec::with_capacity(cap + SALT_SEG.len() + 3 + salt.len());
        s.extend_from_slice(SALT_SEG.as_ref());
        s.extend_from_slice(format!("{}:", salt.len()).as_bytes());
        s.extend_from_slice(salt.as_slice());
        s
    } else {
        Vec::with_capacity(cap)
    };

    s.extend_from_slice(SEQ_SEG.as_ref());
    s.extend_from_slice(format!("{}e", seq).as_bytes());
    s.extend_from_slice(V_SEG.as_ref());
    s.extend_from_slice(format!("{}:", value.len()).as_bytes());
    s.extend_from_slice(value);

    Ok(s)
}

#[allow(clippy::result_unit_err)]
pub fn signable_mutable(mutable: &Mutable) -> Result<Vec<u8>, ()> {
    if let Some(ref val) = mutable.value {
        signable(val, mutable.salt.as_ref(), mutable.seq.unwrap_or_default())
    } else {
        Err(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signable_test() {
        let mutable = Mutable {
            value: Some(b"value".to_vec()),
            signature: None,
            seq: None,
            salt: None,
        };
        let sign = signable_mutable(&mutable).unwrap();

        assert_eq!(
            sign.as_slice(),
            &[51, 58, 115, 101, 113, 105, 48, 101, 49, 58, 118, 53, 58, 118, 97, 108, 117, 101][..]
        );

        assert_eq!(
            String::from_utf8(sign).unwrap().as_str(),
            "3:seqi0e1:v5:value"
        )
    }
}

/// taken from
/// https://github.com/holepunchto/hyperdht/blob/0d4f4b65bf1c252487f7fd52ef9e21ac76a3ceba/lib/constants.js#L42-L54
pub mod namespace {
    macro_rules! const_hex_decode {
        ($arg:expr) => {{
            match const_hex::const_decode_to_array($arg) {
                Ok(x) => x,
                Err(_) => panic!("Failed to decode"),
            }
        }};
    }

    pub const ANNOUNCE: [u8; 32] =
        const_hex_decode!(b"36386adddf9f6fd60db83a6f42fc159d1146aa8644037664230aaa1f0179d49e");
    pub const UNANNOUNCE: [u8; 32] =
        const_hex_decode!(b"ded293cd93fb395e756ecf5fff426529e72c36eacc22e5ed944d9099a2561e3e");
    pub const MUTABLE_PUT: [u8; 32] =
        const_hex_decode!(b"668e823edd5ce7f5338d68bf1161f4a3c28ce437ee2ab49efd30d99366039b1e");
    pub const PEER_HANDSHAKE: [u8; 32] =
        const_hex_decode!(b"14d6d4b49214ab1033ed204976caa258bae9e1e8543b9ad1fd996a910b0c4e3a");
    pub const PEER_HOLEPUNCH: [u8; 32] =
        const_hex_decode!(b"f1191cd5e67b10b54a507033280ed1ff0e12278268d5679c8f93d417210d168b");
}

