//! `mmlsdisk` parsing.

use std::io::BufRead;
use std::process::Command;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};

/// Returns the disks.
///
/// # Errors
///
/// Returns an error if running `mmlsdisk` fails or if parsing its output fails.
pub fn disks<S: AsRef<str>>(fs_name: S) -> Result<Disks> {
    let fs_name = fs_name.as_ref();

    let mut cmd = Command::new("mmlsdisk");
    cmd.arg(fs_name);
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {:?}", cmd))?;

    Disks::from_reader(output.stdout.as_slice())
}

/// Parsed disks.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Disks(Vec<Disk>);

impl Disks {
    fn from_reader<Input: BufRead>(input: Input) -> Result<Self> {
        let mut index = Index::default();
        let mut disks = Self::default();

        for line in input.lines() {
            let line = line?;

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[2] == "HEADER" {
                index = Index::default();
                header_to_index(&tokens, &mut index);
            } else {
                let entry = Disk::from_tokens(&tokens, &index)?;
                disks.0.push(entry);
            }
        }

        Ok(disks)
    }

    /// Returns an [`Iterator`] over the disks.
    pub fn iter(&self) -> std::slice::Iter<Disk> {
        self.0.iter()
    }

    /// Returns the underlying collection.
    // ALLOW false positive: constant functions cannot evaluate destructors
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn into_inner(self) -> Vec<Disk> {
        self.0
    }
}

impl IntoIterator for Disks {
    type Item = Disk;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Extend<Disk> for Disks {
    fn extend<T: IntoIterator<Item = Disk>>(&mut self, iter: T) {
        for elem in iter {
            self.0.push(elem);
        }
    }
}

impl FromIterator<Self> for Disks {
    fn from_iter<I: IntoIterator<Item = Self>>(iter: I) -> Self {
        let mut c = Self::default();

        for i in iter {
            c.extend(i);
        }

        c
    }
}

/// Disk data.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Disk {
    nsd_name: String,
    is_metadata: bool,
    is_objectdata: bool,
    storage_pool: String,
}

impl Disk {
    /// Returns the NSD name of this disk.
    #[must_use]
    pub fn nsd_name(&self) -> &str {
        &self.nsd_name
    }

    /// Returns `true` if this is a metadata disk.
    #[must_use]
    pub const fn is_metadata(&self) -> bool {
        self.is_metadata
    }

    /// Returns `true` if this is an objectdata disk.
    #[must_use]
    pub const fn is_objectdata(&self) -> bool {
        self.is_objectdata
    }

    /// Returns the storage pool this disk is in.
    #[must_use]
    pub fn pool(&self) -> &str {
        &self.storage_pool
    }
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl Disk {
    fn from_tokens(tokens: &[&str], index: &Index) -> Result<Self> {
        let nsd_name_index =
            index.nsd_name.ok_or_else(|| anyhow!("no NSD name index"))?;
        let nsd_name = tokens[nsd_name_index].into();

        let is_metadata_index = index
            .is_metadata
            .ok_or_else(|| anyhow!("no is metadata index"))?;
        let is_metadata = tokens[is_metadata_index].parse::<Bool>()?.as_bool();

        let is_objectdata_index = index
            .is_objectdata
            .ok_or_else(|| anyhow!("no is objectdata index"))?;
        let is_objectdata =
            tokens[is_objectdata_index].parse::<Bool>()?.as_bool();

        let storage_pool_index = index
            .storage_pool
            .ok_or_else(|| anyhow!("no storage pool index"))?;
        let storage_pool = tokens[storage_pool_index].into();

        Ok(Self {
            nsd_name,
            is_metadata,
            is_objectdata,
            storage_pool,
        })
    }
}

#[derive(Debug, Default)]
struct Index {
    nsd_name: Option<usize>,
    is_metadata: Option<usize>,
    is_objectdata: Option<usize>,
    storage_pool: Option<usize>,
}

fn header_to_index(tokens: &[&str], index: &mut Index) {
    for (i, token) in tokens.iter().enumerate() {
        match *token {
            "nsdName" => index.nsd_name = Some(i),
            "metadata" => index.is_metadata = Some(i),
            "data" => index.is_objectdata = Some(i),
            "storagePool" => index.storage_pool = Some(i),
            _ => {}
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
enum Bool {
    Yes,
    No,
}

impl Bool {
    const fn as_bool(self) -> bool {
        match self {
            Self::Yes => true,
            Self::No => false,
        }
    }
}

impl FromStr for Bool {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "yes" => Ok(Self::Yes),
            "no" => Ok(Self::No),
            unknown => Err(anyhow!("unknown bool: {}", unknown)),
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
        let input = include_str!("disk-example.in");

        let fs = Disks::from_reader(input.as_bytes()).unwrap();
        let mut fs = fs.0.into_iter();

        assert_eq!(
            fs.next(),
            Some(Disk {
                nsd_name: "disk1".into(),
                is_metadata: true,
                is_objectdata: false,
                storage_pool: "system".into(),
            })
        );

        assert_eq!(
            fs.next(),
            Some(Disk {
                nsd_name: "disk2".into(),
                is_metadata: false,
                is_objectdata: true,
                storage_pool: "nvme".into(),
            })
        );

        assert_eq!(
            fs.next(),
            Some(Disk {
                nsd_name: "disk3".into(),
                is_metadata: false,
                is_objectdata: true,
                storage_pool: "nlsas".into(),
            })
        );

        assert_eq!(fs.next(), None);
    }
}
