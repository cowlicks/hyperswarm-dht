import DHT from './dht-rpc/index.js'
import crypto from 'crypto';
const VALUES = 0 // define a command enum
const value = 'hello'
function hash (value) {
  return crypto.createHash('sha256').update(value).digest()
}
const opts = { ephemeral: false }
const bufeq = (a, b) => {
  if (!a || !b) return false
  for (let i = 0; i < a.length; i++) {
    if( a[i] != b[i]) return false
  }
  return true
}

if (!bufeq(Buffer.from([1,2]), Buffer.from([1,2]))) {
  throw 'satoheush'
}

//throw new Error('sths')
const node = new DHT({
  bootstrap: [
    //'188.166.28.20:33041',
    //"node1.hyperdht.org:49737",
    '88.99.3.86:49737'
    //"2.0.194.73:25432"
    //'localhost:10001'
  ],
  ...opts
})

await node.fullyBootstrapped();

const target = Buffer.from('3f3f3712d7cc1daf11b6eec48c9c97ae516cb094125c6e7aae009efa66b569fa', 'hex');
const t2 = Buffer.from('3f3f3712d7cc1daf11b6eec48c9c97ae516cb094125c6e7aae009efa66b569fa', 'hex');
if (!bufeq(target, t2)) {
  throw 6666
}

/*
console.log(target);
console.log('DO QUERY');

for await (const data of node.findNode(target)) {
  //console.log(data);
  console.log(data.from);
  if (bufeq(target, data?.from?.id)) {
    console.log('BREAK')
    break
  }
}
*/
/*
for await (const data of node.query({ target, command: 3, value: null })) {
  if (data.value) {
    console.log('got value', data.value);
    if (hash(data.value).toString('hex') === target.toString('hex')) {
    // We found the value! Destroy the query stream as there is no need to continue.
    console.log(target.toString('hex'), '-->', data.value.toString())
    break
    }
  }
}
*/
console.log('(query finished)')
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
sleep(3*1000);

//console.log([...node.toArray()])
