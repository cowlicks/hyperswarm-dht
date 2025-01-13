mod common;
use std::net::SocketAddr;

use hyperdht::crypto::{namespace, sign_announce, Keypair2};

use common::{
    js::{path_to_node_modules, require_js_data},
    Result,
};
use rusty_nodejs_repl::{Config, Repl};

const DEFAULT_SEED: [u8; 32] = [
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
const KEYPAIR_JS: &str = "
createKeyPair = require('hyperdht/lib/crypto.js').createKeyPair;
seed = new Uint8Array(32);
seed[0] = 1;
keyPair = createKeyPair(seed)
";

fn buf_to_js_comparable_str(x: &[u8]) -> String {
    x.iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>()
        .join(",")
}

async fn make_repl() -> Repl {
    require_js_data().unwrap();

    let mut conf = Config::build().unwrap();
    conf.before.push(
        "
stringify = JSON.stringify;
write = process.stdout.write.bind(process.stdout);
"
        .into(),
    );
    conf.path_to_node_modules = Some(path_to_node_modules().unwrap().display().to_string());
    conf.start().await.unwrap()
}

#[tokio::test]
async fn check_namespace() -> crate::Result<()> {
    let mut repl = make_repl().await;
    let result = repl
        .run(
            "
const { NS } = require('hyperdht/lib/constants');
namespace = NS
process.stdout.write([...namespace.ANNOUNCE].toString());
",
        )
        .await?;
    assert_eq!(
        buf_to_js_comparable_str(&namespace::ANNOUNCE),
        String::from_utf8_lossy(&result)
    );
    let result = repl
        .run("process.stdout.write([...namespace.UNANNOUNCE].toString())")
        .await?;
    assert_eq!(
        buf_to_js_comparable_str(&namespace::UNANNOUNCE),
        String::from_utf8_lossy(&result)
    );

    Ok(())
}

#[tokio::test]
async fn keygen() -> Result<()> {
    let mut repl = make_repl().await;
    let result: Vec<Vec<u8>> = repl
        .json_run(
            "
createKeyPair = require('hyperdht/lib/crypto.js').createKeyPair;
let seed = new Uint8Array(32);
seed[0] = 1;
keyPair = createKeyPair(seed)
write(stringify([[...keyPair.publicKey], [...keyPair.secretKey]]))
",
        )
        .await?;

    let k2 = Keypair2::from_seed(DEFAULT_SEED);
    assert_eq!(k2.public.as_slice(), &result[0]);
    assert_eq!(k2.secret.as_slice(), &result[1]);
    Ok(())
}
/// Test creating a signature for an Announce is correct.
/// JS code taken from:
/// https://github.com/holepunchto/hyperdht/blob/0d4f4b65bf1c252487f7fd52ef9e21ac76a3ceba/index.js#L424-L434
#[tokio::test]
async fn test_sign_announce() -> Result<()> {
    let target = [1; 32];
    let token = [2; 32];
    let from_id = [3; 32];

    let mut repl = make_repl().await;
    repl.run(KEYPAIR_JS).await?;
    let expected: Vec<u8> = repl
        .json_run(
            "
Persistent = require('hyperdht/lib/persistent')
c = require('compact-encoding')
m = require('hyperdht/lib/messages')

relayAddresses = [];
target = Buffer.alloc(32).fill(1);
token = Buffer.alloc(32).fill(2);
from_id = Buffer.alloc(32).fill(3);

ann = {
  peer: {
    publicKey: keyPair.publicKey,
    relayAddresses: relayAddresses || []
  },
  refresh: null,
  signature: null
}

signature = await Persistent.signAnnounce(target, token, from_id, ann, keyPair)
write(stringify([...signature]))
",
        )
        .await?;

    let kp = Keypair2::from_seed(DEFAULT_SEED);
    let announce = sign_announce(&kp, target.into(), &token, &from_id, &[])?;
    assert_eq!(announce.signature.0.as_slice(), &expected);
    Ok(())
}
#[tokio::test]
async fn test_sign_announce_with_relays() -> Result<()> {
    let one: SocketAddr = "192.168.1.2:1234".parse().unwrap();
    let two: SocketAddr = "10.11.12.13:6547".parse().unwrap();
    let three: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let relay_addresses = vec![one, two, three];
    let target = [1; 32];
    let token = [2; 32];
    let from_id = [3; 32];

    let mut repl = make_repl().await;
    repl.run(KEYPAIR_JS).await?;
    let expected: Vec<u8> = repl
        .json_run(
            "
Persistent = require('hyperdht/lib/persistent')
c = require('compact-encoding')
m = require('hyperdht/lib/messages')

relayAddresses = [
    {host: '192.168.1.2', port: 1234},
    {host: '10.11.12.13', port: 6547},
    {host: '127.0.0.1', port: 80},
];
target = Buffer.alloc(32).fill(1);
token = Buffer.alloc(32).fill(2);
from_id = Buffer.alloc(32).fill(3);

ann = {
  peer: {
    publicKey: keyPair.publicKey,
    relayAddresses: relayAddresses || []
  },
  refresh: null,
  signature: null
}

signature = await Persistent.signAnnounce(target, token, from_id, ann, keyPair)
write(stringify([...signature]))
",
        )
        .await?;

    let kp = Keypair2::from_seed(DEFAULT_SEED);
    let announce = sign_announce(&kp, target.into(), &token, &from_id, &relay_addresses)?;
    assert_eq!(announce.signature.0.as_slice(), &expected);
    Ok(())
}

/// Test encoding a signed Announce is correct.
/// JS code taken from:
/// https://github.com/holepunchto/hyperdht/blob/0d4f4b65bf1c252487f7fd52ef9e21ac76a3ceba/index.js#L424-L436
#[tokio::test]
async fn test_sign_and_encode_announce() -> Result<()> {
    let target = [1; 32];
    let token = [2; 32];
    let from_id = [3; 32];

    let mut repl = make_repl().await;
    repl.run(KEYPAIR_JS).await?;
    let expected: Vec<u8> = repl
        .json_run(
            "
Persistent = require('hyperdht/lib/persistent')
c = require('compact-encoding')
m = require('hyperdht/lib/messages')

relayAddresses = [];
target = Buffer.alloc(32).fill(1);
token = Buffer.alloc(32).fill(2);
from_id = Buffer.alloc(32).fill(3);

ann = {
  peer: {
    publicKey: keyPair.publicKey,
    relayAddresses: relayAddresses || []
  },
  refresh: null,
  signature: null
}

ann.signature = await Persistent.signAnnounce(target, token, from_id, ann, keyPair)
write(stringify([...c.encode(m.announce, ann)]))
",
        )
        .await?;

    let kp = Keypair2::from_seed(DEFAULT_SEED);
    let announce = sign_announce(&kp, target.into(), &token, &from_id, &[])?;
    use compact_encoding::types::CompactEncodable;
    let mut buff: Vec<u8> = vec![0u8; CompactEncodable::encoded_size(&announce).unwrap()];
    announce.encoded_bytes(&mut buff).unwrap();
    assert_eq!(buff, expected);
    Ok(())
}
#[tokio::test]
async fn test_sign_and_encode_announce_with_relays() -> Result<()> {
    let target = [1; 32];
    let token = [2; 32];
    let from_id = [3; 32];

    let one: SocketAddr = "192.168.1.2:1234".parse().unwrap();
    let two: SocketAddr = "10.11.12.13:6547".parse().unwrap();
    let three: SocketAddr = "127.0.0.1:80".parse().unwrap();
    let relay_addresses = vec![one, two, three];

    let mut repl = make_repl().await;
    repl.run(KEYPAIR_JS).await?;
    let expected: Vec<u8> = repl
        .json_run(
            "
Persistent = require('hyperdht/lib/persistent')
c = require('compact-encoding')
m = require('hyperdht/lib/messages')

relayAddresses = [
    {host: '192.168.1.2', port: 1234},
    {host: '10.11.12.13', port: 6547},
    {host: '127.0.0.1', port: 80},
];
target = Buffer.alloc(32).fill(1);
token = Buffer.alloc(32).fill(2);
from_id = Buffer.alloc(32).fill(3);

ann = {
  peer: {
    publicKey: keyPair.publicKey,
    relayAddresses: relayAddresses || []
  },
  refresh: null,
  signature: null
}

ann.signature = await Persistent.signAnnounce(target, token, from_id, ann, keyPair)
write(stringify([...c.encode(m.announce, ann)]))
",
        )
        .await?;

    let kp = Keypair2::from_seed(DEFAULT_SEED);
    let announce = sign_announce(&kp, target.into(), &token, &from_id, &relay_addresses)?;
    use compact_encoding::types::CompactEncodable;
    let mut buff: Vec<u8> = vec![0u8; CompactEncodable::encoded_size(&announce).unwrap()];
    announce.encoded_bytes(&mut buff).unwrap();
    assert_eq!(buff, expected);
    Ok(())
}
