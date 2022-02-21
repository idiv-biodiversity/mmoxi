//! `mmlsfs` parsing.

use std::io::BufRead;
use std::process::Command;

use anyhow::{anyhow, Context, Result};

/// Returns the file system names.
///
/// # Errors
///
/// Returns an error if running `mmlsfs` fails or if parsing its output fails.
pub fn names() -> Result<Vec<String>> {
    let mut cmd = Command::new("mmlsfs");
    cmd.args(["all", "-Y", "-B"]);

    let output = cmd
        .output()
        .with_context(|| format!("error running: {:?}", cmd))?;

    let data = Filesystems::from_reader(output.stdout.as_slice())?;

    let names = data.0.into_iter().map(|fs| fs.name).collect();

    Ok(names)
}

/// Parsed file systems.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Filesystems(Vec<Filesystem>);

impl Filesystems {
    fn from_reader<Input: BufRead>(input: Input) -> Result<Self> {
        let mut index = Index::default();
        let mut fs = Self::default();

        for line in input.lines() {
            let line = line?;

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[2] == "HEADER" {
                index = Index::default();
                header_to_index(&tokens, &mut index);
            } else {
                let entry = Filesystem::from_tokens(&tokens, &index)?;
                fs.0.push(entry);
            }
        }

        Ok(fs)
    }
}

impl IntoIterator for Filesystems {
    type Item = Filesystem;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// File system data.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Filesystem {
    name: String,
}

impl Filesystem {
    /// Returns the file system name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl Filesystem {
    fn from_tokens(tokens: &[&str], index: &Index) -> Result<Self> {
        let name_index =
            index.name.ok_or(anyhow!("no filesystem name index"))?;
        let name = tokens[name_index].into();

        Ok(Self { name })
    }
}

#[derive(Debug, Default)]
struct Index {
    name: Option<usize>,
}

fn header_to_index(tokens: &[&str], index: &mut Index) {
    for (i, token) in tokens.iter().enumerate() {
        if *token == "deviceName" {
            index.name = Some(i);
        }
    }
}

// ----------------------------------------------------------------------------
// tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let input = include_str!("fs-example.in");

        let fs = Filesystems::from_reader(input.as_bytes()).unwrap();
        let mut fs = fs.0.into_iter();

        assert_eq!(
            fs.next(),
            Some(Filesystem {
                name: "gpfs1".into()
            })
        );

        assert_eq!(
            fs.next(),
            Some(Filesystem {
                name: "gpfs2".into()
            })
        );

        assert_eq!(fs.next(), None);
    }
}
