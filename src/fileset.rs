//! `mmlsfileset` parsing.

use std::io::BufRead;
use std::process::Command;

use anyhow::{anyhow, Context, Result};

/// A fileset.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Fileset {
    filesystem_name: String,
    fileset_name: String,
    max_inodes: u64,
    alloc_inodes: u64,
}

impl Fileset {
    /// Returns the filesystem name.
    #[must_use]
    pub fn filesystem_name(&self) -> &str {
        self.filesystem_name.as_ref()
    }

    /// Returns the fileset name.
    #[must_use]
    pub fn fileset_name(&self) -> &str {
        self.fileset_name.as_ref()
    }

    /// Returns the maximum number of inodes.
    #[must_use]
    pub const fn max_inodes(&self) -> u64 {
        self.max_inodes
    }

    /// Returns the allocated inodes.
    #[must_use]
    pub const fn alloc_inodes(&self) -> u64 {
        self.alloc_inodes
    }
}

/// Returns all filesets of the given file system.
///
/// # Errors
///
/// Returns an error if running `mmlsfileset` fails or if parsing its output
/// fails.
pub fn filesets(fs: &str) -> Result<Vec<Fileset>> {
    let mut cmd = Command::new("mmlsfileset");
    cmd.arg(fs);
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {cmd:?}"))?;

    let filesets = from_reader(output.stdout.as_slice())?;

    Ok(filesets)
}

/// Returns the fileset for the given file system.
///
/// Technically, `mmlsfileset fs fileset -Y` could return multiple filesets.
/// Programmatically, only the first one of that list is returned.
///
/// # Errors
///
/// Returns an error if running `mmlsfileset` fails or if parsing its output
/// fails. Also, if `mmlsfileset` returns no fileset at all, an error is
/// returned.
pub fn fileset(fs: &str, fileset: &str) -> Result<Fileset> {
    let mut cmd = Command::new("mmlsfileset");
    cmd.arg(fs);
    cmd.arg(fileset);
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {cmd:?}"))?;

    let filesets = from_reader(output.stdout.as_slice())?;

    let Some(fileset) = filesets.into_iter().next() else {
        return Err(anyhow!("no fileset returned"));
    };

    Ok(fileset)
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Index {
    filesystem_name: Option<usize>,
    fileset_name: Option<usize>,
    max_inodes: Option<usize>,
    alloc_inodes: Option<usize>,
}

fn from_reader<Input: BufRead>(input: Input) -> Result<Vec<Fileset>> {
    let mut index = Index::default();
    let mut fs = vec![];

    for line in input.lines() {
        let line = line?;

        let tokens = line.split(':').collect::<Vec<_>>();

        if tokens[2] == "HEADER" {
            index = Index::default();
            header_to_index(&tokens, &mut index);
        } else {
            let entry = from_tokens(&tokens, &index)?;
            fs.push(entry);
        }
    }

    Ok(fs)
}

fn from_tokens(tokens: &[&str], index: &Index) -> Result<Fileset> {
    let filesystem_name_index = index
        .filesystem_name
        .ok_or_else(|| anyhow!("no filesystemName index"))?;
    let filesystem_name = tokens[filesystem_name_index].into();

    let fileset_name_index = index
        .fileset_name
        .ok_or_else(|| anyhow!("no filesetName index"))?;
    let fileset_name = tokens[fileset_name_index].into();

    let max_inodes_index = index
        .max_inodes
        .ok_or_else(|| anyhow!("no maxInodes index"))?;
    let max_inodes = tokens[max_inodes_index]
        .parse()
        .with_context(|| "parsing maxInodes value")?;

    let alloc_inodes_index = index
        .alloc_inodes
        .ok_or_else(|| anyhow!("no allocInodes index"))?;
    let alloc_inodes = tokens[alloc_inodes_index]
        .parse()
        .with_context(|| "parsing allocInodes value")?;

    Ok(Fileset {
        filesystem_name,
        fileset_name,
        max_inodes,
        alloc_inodes,
    })
}

fn header_to_index(tokens: &[&str], index: &mut Index) {
    for (i, token) in tokens.iter().enumerate() {
        match *token {
            "filesystemName" => index.filesystem_name = Some(i),
            "filesetName" => index.fileset_name = Some(i),
            "maxInodes" => index.max_inodes = Some(i),
            "allocInodes" => index.alloc_inodes = Some(i),
            _ => {}
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
        let input = include_str!("fileset-example.in");

        let filesets = from_reader(input.as_bytes()).unwrap();
        let mut filesets = filesets.into_iter();

        assert_eq!(
            filesets.next(),
            Some(Fileset {
                filesystem_name: "gpfs1".into(),
                fileset_name: "public".into(),
                max_inodes: 20_971_520,
                alloc_inodes: 5_251_072,
            })
        );

        assert_eq!(
            filesets.next(),
            Some(Fileset {
                filesystem_name: "gpfs1".into(),
                fileset_name: "work".into(),
                max_inodes: 295_313_408,
                alloc_inodes: 260_063_232,
            })
        );

        assert_eq!(filesets.next(), None);
    }
}
