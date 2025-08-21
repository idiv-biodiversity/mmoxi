//! `mmlsfileset` parsing.

use std::io::{BufRead, Write};
use std::process::Command;

use anyhow::{anyhow, Context, Result};

use crate::prom::ToText;
use crate::util::MMBool;

/// A fileset.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Fileset {
    name: String,
    filesystem_name: String,
    is_inode_space_owner: bool,
    max_inodes: u64,
    alloc_inodes: u64,
    comment: Option<String>,
}

impl Fileset {
    /// Returns the filesystem name.
    #[must_use]
    pub fn filesystem_name(&self) -> &str {
        self.filesystem_name.as_ref()
    }

    /// Returns the fileset name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Returns if this fileset is the owner of its inode space.
    #[must_use]
    pub const fn is_inode_space_owner(&self) -> bool {
        self.is_inode_space_owner
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

    /// Optionally returns the comment.
    #[must_use]
    pub const fn comment(&self) -> Option<&String> {
        self.comment.as_ref()
    }
}

impl ToText for Vec<Fileset> {
    fn to_prom(&self, output: &mut impl Write) -> Result<()> {
        writeln!(
            output,
            "# HELP gpfs_fileset_max_inodes GPFS fileset maximum inodes"
        )?;
        writeln!(output, "# TYPE gpfs_fileset_max_inodes gauge")?;

        for fileset in self.iter().filter(|f| f.is_inode_space_owner()) {
            writeln!(
                output,
                "gpfs_fileset_max_inodes{{fs=\"{}\",fileset=\"{}\"}} {}",
                fileset.filesystem_name(),
                fileset.name(),
                fileset.max_inodes(),
            )?;
        }

        writeln!(
            output,
            "# HELP gpfs_fileset_alloc_inodes GPFS fileset allocated inodes"
        )?;
        writeln!(output, "# TYPE gpfs_fileset_alloc_inodes gauge")?;

        for fileset in self.iter().filter(|f| f.is_inode_space_owner()) {
            writeln!(
                output,
                "gpfs_fileset_alloc_inodes{{fs=\"{}\",fileset=\"{}\"}} {}",
                fileset.filesystem_name(),
                fileset.name(),
                fileset.alloc_inodes(),
            )?;
        }

        Ok(())
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
    is_inode_space_owner: Option<usize>,
    max_inodes: Option<usize>,
    alloc_inodes: Option<usize>,
    comment: Option<usize>,
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

    let is_inode_space_owner_index = index
        .is_inode_space_owner
        .ok_or_else(|| anyhow!("no isInodeSpaceOwner index"))?;
    let is_inode_space_owner = tokens[is_inode_space_owner_index]
        .parse::<MMBool>()
        .with_context(|| "parsing isInodeSpaceOwner value")?
        .as_bool();

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

    let comment_index =
        index.comment.ok_or_else(|| anyhow!("no comment index"))?;
    let comment = tokens[comment_index].replace("%3A", ":");
    let comment = Some(comment).filter(|s| !s.is_empty());

    Ok(Fileset {
        name: fileset_name,
        filesystem_name,
        is_inode_space_owner,
        max_inodes,
        alloc_inodes,
        comment,
    })
}

fn header_to_index(tokens: &[&str], index: &mut Index) {
    for (i, token) in tokens.iter().enumerate() {
        match *token {
            "filesystemName" => index.filesystem_name = Some(i),
            "filesetName" => index.fileset_name = Some(i),
            "isInodeSpaceOwner" => index.is_inode_space_owner = Some(i),
            "maxInodes" => index.max_inodes = Some(i),
            "allocInodes" => index.alloc_inodes = Some(i),
            "comment" => index.comment = Some(i),
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
                name: "public".into(),
                filesystem_name: "gpfs1".into(),
                is_inode_space_owner: true,
                max_inodes: 20_971_520,
                alloc_inodes: 5_251_072,
                comment: None,
            })
        );

        assert_eq!(
            filesets.next(),
            Some(Fileset {
                name: "work".into(),
                filesystem_name: "gpfs1".into(),
                is_inode_space_owner: true,
                max_inodes: 295_313_408,
                alloc_inodes: 260_063_232,
                comment: None,
            })
        );

        assert_eq!(
            filesets.next(),
            Some(Fileset {
                name: "data_foo".into(),
                filesystem_name: "gpfs1".into(),
                is_inode_space_owner: true,
                max_inodes: 20_000_768,
                alloc_inodes: 1_032_192,
                comment: Some("end of project: 2026-11".into()),
            })
        );

        assert_eq!(
            filesets.next(),
            Some(Fileset {
                name: "data_db".into(),
                filesystem_name: "gpfs1".into(),
                is_inode_space_owner: true,
                max_inodes: 20_971_520,
                alloc_inodes: 5_251_072,
                comment: Some("end of project: 2042-12".into()),
            })
        );

        assert_eq!(
            filesets.next(),
            Some(Fileset {
                name: "data_db_foo".into(),
                filesystem_name: "gpfs1".into(),
                is_inode_space_owner: false,
                max_inodes: 0,
                alloc_inodes: 0,
                comment: Some("end of project: 2030-12".into()),
            })
        );

        assert_eq!(filesets.next(), None);
    }
}
