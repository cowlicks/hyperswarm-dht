import DHT from './dht-rpc/index.js'
import crypto from 'crypto';
const VALUES = 0 // define a command enum
const value = 'hello'
function hash (value) {
  return crypto.createHash('sha256').update(value).digest()
}
const opts = { ephemeral: false };

const N_NODES = 1;


const nodes = [];
// Let's create 100 dht nodes for our example.
for (var i = 0; i < N_NODES; i++) {
  process.stdout.write(String(i));
  const n = await createNode(i)
  nodes.push(n);
}

async function createNode () {
  const node = new DHT({
    bootstrap: [
'188.166.28.20:33041',
'129.151.241.61:49737',
'136.243.5.20:39876',
'88.99.3.86:53521',
    ],
    ...opts
  })
  await node.fullyBootstrapped();
  console.log('BOOTSTRAPPED!', i);
  //let x = await node.findNode(Buffer.from('hello world'));
  //console.log(x);
  //const q = node.query({ target: Buffer.from('asnotehusntahoesunthsanteohunthe'), command: 2, value: null});
  console.log('sthsh');
  for await (const d of node.refresh()) {
    console.log('xxx');
    console.log(d);
    console.log([...node.toArray()]);
  }

  const values = new Map()
  const VALUES = 0 // define a command enum

  console.log(node.address());
  console.log([...node.toArray()]);

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
}

