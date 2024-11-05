import  DHT from 'dht-rpc';
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

await node.fullyBootstrapped();

const target = hash(value);
console.log(target);
for await (const data of node.query({ target, command: VALUES })) {
  if (data.value) {
    console.log('got value', data.value);
    if (hash(data.value).toString('hex') === target.toString('hex')) {
    // We found the value! Destroy the query stream as there is no need to continue.
    console.log(target.toString('hex'), '-->', data.value.toString())
    break
    }
  }
}
console.log('(query finished)')
