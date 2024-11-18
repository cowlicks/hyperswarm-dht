import DHT from './dht-rpc/index.js'
import crypto from 'crypto';
const VALUES = 0 // define a command enum
const value = 'hello'
function hash (value) {
  return crypto.createHash('sha256').update(value).digest()
}
const opts = { ephemeral: false, host: '127.0.0.1', }

const node = new DHT({
  bootstrap: [
    'localhost:10001'
  ],
  ...opts
})
console.log(0);

await node.fullyBootstrapped();
console.log(0);

const target  = hash(value);
const q = node.query({
  target: hash(value),
  command: VALUES,
  value: Buffer.from(value),
}, {
  // commit true will make the query re-request the 20 closest
  // nodes with a valid round trip token to update the values
  commit: true
})
console.log(0);

await q.finished()
console.log('putted');
