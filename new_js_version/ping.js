import  DHT from 'dht-rpc';
import crypto from 'crypto';
const VALUES = 0 // define a command enum
const value = 'hello'
function hash (value) {
  return crypto.createHash('sha256').update(value).digest()
}
const opts = { ephemeeral: false, host: '127.0.0.1', }
const p = (x) => [console.log(x), x][1]

const node = new DHT({
  bootstrap: [
    'localhost:10001'
  ],
  ...opts
})
console.log(0);

await node.fullyBootstrapped();

p(await node.ping({ host: '127.0.0.1', port: 44356 }));
p(await node.ping({ host: '127.0.0.1', port: 10001 }));
