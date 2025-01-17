use std::net::SocketAddr;

use blake2::VarBlake2b;
use compact_encoding::types::{write_array, CompactEncodable};
use ed25519_dalek::SignatureError;
pub use ed25519_dalek::{ExpandedSecretKey, Keypair, PublicKey, SecretKey, Signature, Verifier};
use libsodium_sys::{
    crypto_sign_BYTES, crypto_sign_PUBLICKEYBYTES, crypto_sign_SECRETKEYBYTES,
    crypto_sign_SEEDBYTES, crypto_sign_keypair, crypto_sign_seed_keypair,
};

use crate::{cenc::Announce, dht_proto::Mutable};
use ::dht_rpc::IdBytes;

/// VALUE_MAX_SIZE + packet overhead (i.e. the key etc.)
/// should be less than the network MTU, normally 1400 bytes
pub const VALUE_MAX_SIZE: usize = 1000;

const SALT_SEG: &[u8; 6] = b"4:salt";
const SEQ_SEG: &[u8; 6] = b"3:seqi";
const V_SEG: &[u8; 3] = b"1:v";

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

/// hash the `val` with a key size of U32 and put it into [`IdBytes`]
pub fn hash_id(val: &[u8]) -> IdBytes {
    use blake2::digest::{Update, VariableOutput};
    let mut key = [0; 32];
    let mut hasher = VarBlake2b::new(32).unwrap();
    hasher.update(val);
    hasher.finalize_variable(|res| key.copy_from_slice(res));
    key.into()
}

#[derive(Debug)]
pub struct Keypair2 {
    /// The public half of this keypair.
    pub public: [u8; crypto_sign_PUBLICKEYBYTES as usize],
    /// The secret half of this keypair.
    pub secret: [u8; crypto_sign_SECRETKEYBYTES as usize],
}

impl Keypair2 {
    // TODO make this the sign function

    fn new() -> Self {
        let mut public = [0; crypto_sign_PUBLICKEYBYTES as usize];
        let mut secret = [0; crypto_sign_SECRETKEYBYTES as usize];
        let err = unsafe { crypto_sign_keypair(public.as_mut_ptr(), secret.as_mut_ptr()) };
        if err != 0 {
            todo!()
        }
        Self { public, secret }
    }

    pub fn from_seed(seed: [u8; crypto_sign_SEEDBYTES as usize]) -> Self {
        let mut public = [0; crypto_sign_PUBLICKEYBYTES as usize];
        let mut secret = [0; crypto_sign_SECRETKEYBYTES as usize];
        let err = unsafe {
            crypto_sign_seed_keypair(public.as_mut_ptr(), secret.as_mut_ptr(), seed.as_ptr())
        };
        if err != 0 {
            todo!()
        }
        Self { public, secret }
    }
    pub fn sign(&self, value: &[u8]) -> Signature2 {
        let mut signature: [u8; 64] = [0u8; crypto_sign_BYTES as usize];
        let err = unsafe {
            libsodium_sys::crypto_sign_detached(
                signature.as_mut_ptr(),
                std::ptr::null_mut(),
                value.as_ptr(),
                value.len() as _,
                self.secret.as_ptr(),
            )
        };
        if err != 0 {
            todo!()
        }
        Signature2(signature)
    }
}

#[derive(Debug)]
pub struct Signature2(pub [u8; crypto_sign_BYTES as usize]);
/*
impl Default for Keypair2 {
    fn default() -> Self {
        Self {
            secret: Default::default(),
            public: Default::default(),
        }
    }
}
*/
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

/// taken from
/// https://github.com/cowlicks/hyperdht/blob/eecdf3669744e88ec2fceb851cedf5274a106c94/test/print_ns.js#L2
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
        const_hex_decode!(b"36386adddf9f6fd60db83a6f42fc159d1146aa8644037664230aaa1f0179d497");
    pub const UNANNOUNCE: [u8; 32] =
        const_hex_decode!(b"ded293cd93fb395e756ecf5fff426529e72c36eacc22e5ed944d9099a2561e32");
    pub const MUTABLE_PUT: [u8; 32] =
        const_hex_decode!(b"668e823edd5ce7f5338d68bf1161f4a3c28ce437ee2ab49efd30d99366039b1e");
    pub const PEER_HANDSHAKE: [u8; 32] =
        const_hex_decode!(b"14d6d4b49214ab1033ed204976caa258bae9e1e8543b9ad1fd996a910b0c4e3a");
    pub const PEER_HOLEPUNCH: [u8; 32] =
        const_hex_decode!(b"f1191cd5e67b10b54a507033280ed1ff0e12278268d5679c8f93d417210d168b");
}

pub fn generic_hash_batch(inputs: &[&[u8]]) -> [u8; 32] {
    let mut out = [0u8; libsodium_sys::crypto_generichash_BYTES as usize];
    let mut st = vec![0u8; unsafe { libsodium_sys::crypto_generichash_statebytes() }];
    let pst = unsafe {
        std::mem::transmute::<*mut u8, *mut libsodium_sys::crypto_generichash_state>(
            st.as_mut_ptr(),
        )
    };

    if 0 != unsafe {
        libsodium_sys::crypto_generichash_init(pst, std::ptr::null_mut(), 0, out.len())
    } {
        panic!("Should only error when out-of-memory OR when the input is invalid. Inputs here or checked");
    }

    for chunk in inputs {
        if 0 != unsafe {
            libsodium_sys::crypto_generichash_update(pst, chunk.as_ptr(), chunk.len() as u64)
        } {
            panic!("Should only error when out-of-memory OR when the input is invalid. Inputs here or checked");
        }
    }
    if 0 != unsafe { libsodium_sys::crypto_generichash_final(pst, out.as_mut_ptr(), out.len()) } {
        panic!("Should only error when out-of-memory OR when the input is invalid. Inputs here or checked");
    }
    out
}

pub fn sign_announce_or_unannounce(
    keypair: &Keypair2,
    target: IdBytes,
    token: &[u8; 32],
    from_id: &[u8; 32],
    relay_addresses: &[SocketAddr],
    namespace: &[u8; 32],
) -> crate::Result<Announce> {
    use crate::cenc::Peer;
    let peer = Peer {
        public_key: keypair.public,
        relay_addresses: relay_addresses.to_vec(),
    };
    let mut encoded_peer = vec![0; peer.encoded_size()?];
    peer.encoded_bytes(&mut encoded_peer)?;

    let signable = {
        let mut signable = [0; 64];
        let rest = write_array::<32>(namespace, &mut signable)?;
        rest.copy_from_slice(&generic_hash_batch(&[
            &target.0,
            from_id,
            token,
            &encoded_peer,
            &[],
        ]));
        signable
    };
    Ok(Announce {
        peer,
        refresh: None,
        signature: keypair.sign(&signable),
    })
}

#[cfg(test)]
mod test {
    use ed25519_dalek::ed25519::signature::Keypair;

    use super::*;

    #[test]
    fn compare_batch_hash_with_javascript_result() {
        // value calculated from
        // https://gist.github.com/cowlicks/901ee94ace3b881e0ef057f472d16a71
        let expected: [u8; 32] = [
            96, 83, 105, 30, 76, 247, 143, 215, 26, 251, 250, 184, 48, 122, 222, 187, 105, 4, 254,
            251, 46, 29, 249, 66, 167, 216, 198, 209, 204, 167, 180, 62,
        ];

        let x: &[&[u8]] = &[b"yolo", b"wassup", b"howdy"];
        let res = generic_hash_batch(x);
        assert_eq!(res, expected);
    }

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
