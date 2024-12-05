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

use std::fmt;

pub(crate) fn debug_vec<T: fmt::Debug>(vec: &[T]) -> impl fmt::Debug + '_ {
    struct VecFormatter<'a, T>(&'a [T]);

    impl<'a, T: fmt::Debug> fmt::Debug for VecFormatter<'a, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if self.0.len() <= 4 {
                return f.debug_list().entries(self.0).finish();
            }

            if f.alternate() {
                writeln!(f, "[")?;
                for item in self.0.iter().take(3) {
                    writeln!(f, "    {:#?},", item)?;
                }
                writeln!(f, "    ...")?;
                writeln!(f, "    {:#?}", self.0.last().unwrap())?;
                write!(f, "]")
            } else {
                write!(f, "[")?;
                for item in self.0.iter().take(3) {
                    write!(f, "{:?}, ", item)?;
                }
                write!(f, "..., ")?;
                write!(f, "{:?}]", self.0.last().unwrap())
            }
        }
    }

    VecFormatter(vec)
}
