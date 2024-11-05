import DHT from './dht-rpc/index.js'
import crypto from 'crypto';
const VALUES = 0 // define a command enum
const value = 'hello'
function hash (value) {
  return crypto.createHash('sha256').update(value).digest()
}
const opts = { ephemeral: false, host: '127.0.0.1', }



const nodes = [];
// Let's create 100 dht nodes for our example.
for (var i = 0; i < 1; i++) {
  process.stdout.write(String(i));
  const n = await createNode()
  nodes.push(n);
}

async function createNode () {
  const node = new DHT({
    bootstrap: [
      'localhost:10001'
    ],
    ...opts
  })

  /*
  await node.fullyBootstrapped();

  const values = new Map()
  const VALUES = 0 // define a command enum

  console.log(node.address());

  node.on('request', function (req) {
    console.log('req.command =', req.command);
    if (req.command === VALUES) {
      if (req.token) { // if we are the closest node store the value (ie the node sent a valid roundtrip token)
        console.log(req.token);
        if (req.value) {
          console.log(req.value);
          const key = hash(req.value).toString('hex')
          values.set(key, req.value)
          console.log('Storing', key, '-->', req.value.toString())
          return req.reply(null)
        }
        return req.reply(null)
      }

      const value = values.get(req.target.toString('hex'))
      req.reply(value)
    }
  })
  return node;
  */
}

