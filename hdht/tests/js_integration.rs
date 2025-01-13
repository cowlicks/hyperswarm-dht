mod common;
use hyperdht::crypto::namespace;

use common::{
    js::{path_to_node_modules, require_js_data},
    Result,
};
use rusty_nodejs_repl::Config;

fn buf_to_js_comparable_str(x: &[u8]) -> String {
    x.iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>()
        .join(",")
}

#[tokio::test]
async fn check_namespace() -> crate::Result<()> {
    require_js_data()?;
    let mut conf = Config::build()?;
    conf.path_to_node_modules = Some(path_to_node_modules()?.display().to_string());
    conf.imports.push(
        "
"
        .into(),
    );
    let mut repl = conf.start().await?;
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
