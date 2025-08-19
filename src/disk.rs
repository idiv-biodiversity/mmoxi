//! `mmlsdisk` parsing.

use std::collections::HashMap;
use std::fmt::Display;
use std::io::{BufRead, Write};
use std::process::Command;
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};

use crate::util::MMBool;

/// Returns the disks.
///
/// # Errors
///
/// Returns an error if running `mmlsdisk` fails or if parsing its output fails.
pub fn disks(fs_name: impl AsRef<str>) -> Result<Disks> {
    let fs_name = fs_name.as_ref();

    let mut cmd = Command::new("mmlsdisk");
    cmd.arg(fs_name);
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {cmd:?}"))?;

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
    pub fn iter(&self) -> std::slice::Iter<'_, Disk> {
        self.0.iter()
    }

    /// Returns the underlying collection.
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

impl<'a> IntoIterator for &'a Disks {
    type Item = &'a Disk;
    type IntoIter = std::slice::Iter<'a, Disk>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
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

/// Disk availability.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
#[non_exhaustive]
pub enum Availability {
    /// Disk is available for I/O operations.
    Up,

    /// No I/O operations can be performed.
    Down,

    /// Intermediate state for disks coming up.
    Recovering,

    /// Disk was not successfully brought up.
    Unrecovered,

    /// Unknown state.
    Unknown(String),
}

impl Display for Availability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Up => "up",
            Self::Down => "down",
            Self::Recovering => "recovering",
            Self::Unrecovered => "unrecovered",
            Self::Unknown(s) => s.as_str(),
        };

        write!(f, "{s}")
    }
}

impl FromStr for Availability {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "up" => Ok(Self::Up),
            "down" => Ok(Self::Down),
            "recovering" => Ok(Self::Recovering),
            "unrecovered" => Ok(Self::Unrecovered),
            unknown => Ok(Self::Unknown(unknown.into())),
        }
    }
}

/// Disk data.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Disk {
    nsd_name: String,
    is_metadata: bool,
    is_objectdata: bool,
    availability: Availability,
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
        let is_metadata =
            tokens[is_metadata_index].parse::<MMBool>()?.as_bool();

        let is_objectdata_index = index
            .is_objectdata
            .ok_or_else(|| anyhow!("no is objectdata index"))?;
        let is_objectdata =
            tokens[is_objectdata_index].parse::<MMBool>()?.as_bool();

        let availability_index = index
            .availability
            .ok_or_else(|| anyhow!("no availability index"))?;
        let availability =
            tokens[availability_index].parse::<Availability>()?;

        let storage_pool_index = index
            .storage_pool
            .ok_or_else(|| anyhow!("no storage pool index"))?;
        let storage_pool = tokens[storage_pool_index].into();

        Ok(Self {
            nsd_name,
            is_metadata,
            is_objectdata,
            availability,
            storage_pool,
        })
    }
}

#[derive(Debug, Default)]
struct Index {
    nsd_name: Option<usize>,
    is_metadata: Option<usize>,
    is_objectdata: Option<usize>,
    availability: Option<usize>,
    storage_pool: Option<usize>,
}

fn header_to_index(tokens: &[&str], index: &mut Index) {
    for (i, token) in tokens.iter().enumerate() {
        match *token {
            "nsdName" => index.nsd_name = Some(i),
            "metadata" => index.is_metadata = Some(i),
            "data" => index.is_objectdata = Some(i),
            "availability" => index.availability = Some(i),
            "storagePool" => index.storage_pool = Some(i),
            _ => {}
        }
    }
}

// ----------------------------------------------------------------------------
// prometheus
// ----------------------------------------------------------------------------

impl<S: ::std::hash::BuildHasher> crate::prom::ToText
    for HashMap<String, Disks, S>
{
    fn to_prom(&self, output: &mut impl Write) -> Result<()> {
        for (fs, disks) in self {
            writeln!(
                output,
                "# HELP gpfs_disk_availability GPFS disk availability."
            )?;
            writeln!(output, "# TYPE gpfs_disk_availability gauge")?;

            for disk in &disks.0 {
                let status = match disk.availability {
                    Availability::Up => 0,
                    _ => 1,
                };

                writeln!(
                output,
                "gpfs_disk_availability{{name=\"{}\",fs=\"{}\",pool=\"{}\",availability=\"{}\"}} {}",
                disk.nsd_name,
                fs,
                disk.storage_pool,
                disk.availability,
                status,
            )?;
            }
        }

        Ok(())
    }
}

// ----------------------------------------------------------------------------
// tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prom::ToText;

    #[test]
    fn parse() {
        let input = include_str!("disk-example.in");

        let disks = Disks::from_reader(input.as_bytes()).unwrap();
        let mut disks = disks.0.into_iter();

        assert_eq!(
            disks.next(),
            Some(Disk {
                nsd_name: "disk1".into(),
                is_metadata: true,
                is_objectdata: false,
                availability: Availability::Up,
                storage_pool: "system".into(),
            })
        );

        assert_eq!(
            disks.next(),
            Some(Disk {
                nsd_name: "disk2".into(),
                is_metadata: false,
                is_objectdata: true,
                availability: Availability::Down,
                storage_pool: "nvme".into(),
            })
        );

        assert_eq!(
            disks.next(),
            Some(Disk {
                nsd_name: "disk3".into(),
                is_metadata: false,
                is_objectdata: true,
                availability: Availability::Recovering,
                storage_pool: "nlsas".into(),
            })
        );

        assert_eq!(
            disks.next(),
            Some(Disk {
                nsd_name: "disk4".into(),
                is_metadata: false,
                is_objectdata: true,
                availability: Availability::Unrecovered,
                storage_pool: "nlsas".into(),
            })
        );

        assert_eq!(disks.next(), None);
    }

    #[test]
    fn prometheus() {
        let disks = vec![
            Disk {
                nsd_name: "disk1".into(),
                is_metadata: true,
                is_objectdata: false,
                availability: Availability::Up,
                storage_pool: "system".into(),
            },
            Disk {
                nsd_name: "disk2".into(),
                is_metadata: false,
                is_objectdata: true,
                availability: Availability::Down,
                storage_pool: "nvme".into(),
            },
            Disk {
                nsd_name: "disk3".into(),
                is_metadata: false,
                is_objectdata: true,
                availability: Availability::Recovering,
                storage_pool: "nlsas".into(),
            },
            Disk {
                nsd_name: "disk4".into(),
                is_metadata: false,
                is_objectdata: true,
                availability: Availability::Unrecovered,
                storage_pool: "nlsas".into(),
            },
        ];

        let mut all_disks = HashMap::new();
        all_disks.insert(String::from("gpfs1"), Disks(disks));

        let mut output = vec![];
        all_disks.to_prom(&mut output).unwrap();

        let metrics = std::str::from_utf8(output.as_slice()).unwrap();

        let expected = include_str!("disk-example.prom");
        assert_eq!(metrics, expected);
    }
}
