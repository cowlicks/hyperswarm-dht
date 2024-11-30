use std::fmt::Write;
/// Prettify a byte slice.
pub fn pretty_bytes(bytes: &[u8]) -> String {
    if bytes.len() > 4 {
        let head = bytes[..3].iter().fold(String::new(), |mut output, b| {
            let _ = write!(output, "{b:02x}");
            output
        });
        let tail = bytes[(bytes.len() - 1)..]
            .iter()
            .fold(String::new(), |mut output, b| {
                let _ = write!(output, "{b:02x}");
                output
            });
        format!("{head}..{tail}")
    } else {
        bytes.iter().fold(String::new(), |mut output, b| {
            let _ = write!(output, "{b:02x}");
            output
        })
    }
}
