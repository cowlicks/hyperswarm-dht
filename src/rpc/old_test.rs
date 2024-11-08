use rusty_nodejs_repl::{
    //pipe::{rust_js_stream, RsJsStreamBuilder},
    ConfigBuilder,
};
const PATH_OLD_JS_NODE_MODULES: &str = "~/git/hyper/hyperswarm-dht/old_js_version/node_modules";
const JS_IMPORTS: &str = "
dht = require('dht-rpc');
tape = require('tape');

async function bootstrap() {
    const node = dht({
        ephemeral: true
    })
    return new Promise((resolve, reject) => {
        node.listen(0, function() {
            resolve([node.address().port, node])
        })
    })
}

function cmp(a, b) {
    if (a.length != b.length) {
        return false
    }
    for (let i =0; i < a.length; i++) {
        if (a[i] != b[i]) {
        return false
        }
        
    }
    return true
}

";

const TEST: &str = "
await (async () => {
    const [port, node] = await bootstrap();
    const a = dht({ bootstrap: port })
    const b = dht({ bootstrap: port })

    a.command('echo', {
        query(data, callback) {
            console.error('should not query')
            callback(new Error('nope'))
        },
        update(data, callback) {
            if (!cmp(data.value, Buffer.from('Hello, World!'))) {
                console.error(tape.same(data.value, Buffer.from('Hello, World!')));
                console.error(data.value);
                console.error(Buffer.from('Hello, World!'));
                console.error('expected data wrong')
            }
            callback(null, data.value)
        }
    })

    return (new Promise((resolve, reject) => {
        a.ready(() => {
            b.update('echo', a.id, Buffer.from('Hello, World!'), (err, responses) => {
                a.destroy()
                b.destroy()
                node.destroy()

                if (err) {
                    console.error('got err')
                    return reject();
                }
                //t.error(err, 'no errors')
                if (responses.length != 1) {
                    console.error('one response')
                    return reject();
                }

                if (!cmp(responses[0].value, Buffer.from('Hello, World!'))) {
                    console.error('echoed data')
                    return reject();
                }
                resolve()

            })
        })
    }).catch((...x) => console.error(...x)));
})()
";

#[ignore]
#[tokio::test]
async fn foo() -> Result<(), Box<dyn std::error::Error>> {
    let mut repl = ConfigBuilder::default()
        .path_to_node_modules(Some(PATH_OLD_JS_NODE_MODULES.into()))
        .imports(vec![JS_IMPORTS.to_string()])
        .build()?
        .start()
        .await?;

    let res = repl.run(TEST).await?;
    dbg!(res);
    Ok(())
}
