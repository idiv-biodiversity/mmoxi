//! `mmlsnsd` parsing.

use std::borrow::Cow;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Context, Result};

/// Returns the default local device cache path.
pub const DEFAULT_LOCAL_DEVICE_CACHE: &str = "/run/mmlocal-nsd-device-cache";

/// Returns all NSDs.
///
/// # Errors
///
/// Returns an error if running `mmlsnsd` fails or if parsing its output fails.
pub fn all() -> Result<Nsds> {
    let mut cmd = Command::new("mmlsnsd");
    cmd.args(["-X", "-Y"]);

    let output = cmd
        .output()
        .with_context(|| format!("error running: {:?}", cmd))?;

    Nsds::from_reader(output.stdout.as_slice())
}

/// Returns local NSDs.
///
/// **Note:** This command runs `mmlsnsd -X`, which is an expensive operation.
/// It is recommended to use the [`local_cached`] version of this function.
///
/// # Errors
///
/// Returns an error if either fetching the local node name or fetching the NSD
/// list fails.
pub fn local() -> Result<Nsds> {
    let node = crate::state::local_node_name()
        .context("determining local node name")?;

    let mut nsds = crate::nsd::all()?;
    nsds.0.retain(|nsd| nsd.server_list().contains(&node));
    nsds.0.shrink_to_fit();

    Ok(nsds)
}

/// Returns local NSDs utilizing a cache.
///
/// If the `cache` file exists, try reading from that. If it doesn't exist,
/// fetch the [`local`] NSDs, fill the cache and return the result. Recreating
/// the cache can be en`force`d.
///
/// # Errors
///
/// Returns an error if fetching the [`local`] NSDs fails, or if I/O to the
/// cache file fails.
pub fn local_cached<Cache>(cache: Cache, force: bool) -> Result<Nsds>
where
    Cache: AsRef<Path>,
{
    let cache = cache.as_ref();

    if force || !cache.exists() {
        write_cache(cache)
    } else {
        read_cache(cache)
    }
}

/// Returns NSDs read from cache. Assumes cache exists.
fn read_cache(cache: &Path) -> Result<Nsds> {
    let node = crate::state::local_node_name()
        .context("determining local node name")?;

    let cache = File::open(cache)
        .with_context(|| format!("opening cache file: {}", cache.display()))?;

    let cache = BufReader::new(cache);

    let mut nsds = Nsds::default();

    for line in cache.lines() {
        let line = line?;
        let mut tokens = line.split(':');

        let name = tokens
            .next()
            .ok_or_else(|| anyhow!("no name token"))?
            .into();
        let server_list = vec![node.clone()];
        let device = tokens
            .next()
            .ok_or_else(|| anyhow!("no device token"))?
            .into();

        let nsd = Nsd {
            name,
            server_list,
            device,
        };

        nsds.0.push(nsd);
    }

    Ok(nsds)
}

fn write_cache(cache: &Path) -> Result<Nsds> {
    let nsds = crate::nsd::local().context("fetching local NSD devices")?;

    let cache = File::create(cache).with_context(|| {
        format!("creating cache file: {}", cache.display())
    })?;

    let mut cache = BufWriter::new(cache);

    for nsd in nsds.iter() {
        writeln!(cache, "{}:{}", nsd.name(), nsd.device())?;
    }

    Ok(nsds)
}

// ----------------------------------------------------------------------------
// grouping
// ----------------------------------------------------------------------------

/// File system and pool tuple.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct FsPoolId {
    fs: String,
    pool: String,
}

impl FsPoolId {
    /// Returns the file system.
    #[must_use]
    pub fn fs(&self) -> &str {
        &self.fs
    }

    /// Returns the pool.
    #[must_use]
    pub fn pool(&self) -> &str {
        &self.pool
    }
}

/// Data for NSDs grouped by [`FsPoolId`].
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct ByPool(HashMap<FsPoolId, Nsds>);

impl IntoIterator for ByPool {
    type Item = (FsPoolId, Nsds);
    type IntoIter = std::collections::hash_map::IntoIter<FsPoolId, Nsds>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Returns NSDs grouped by [`FsPoolId`].
///
/// # Errors
///
/// Returns an error if either of the used `mm*` commands fails or if writing
/// to the output stream fails.
pub fn local_pooled<Cache>(device_cache: Cache, force: bool) -> Result<ByPool>
where
    Cache: AsRef<Path>,
{
    let nsds = local_cached(device_cache, force)?.into_inner();

    let mut pooled = ByPool::default();

    for fs in crate::fs::names()? {
        let disks = crate::disk::disks(&fs).with_context(|| {
            format!("fetching disks for file system {}", fs)
        })?;

        for nsd in &nsds {
            if let Some(disk) =
                disks.iter().find(|disk| disk.nsd_name() == nsd.name())
            {
                let id = FsPoolId {
                    fs: (&fs).into(),
                    pool: disk.pool().into(),
                };

                let devices = pooled.0.entry(id).or_default();
                devices.0.push(nsd.clone());
            }
        }
    }

    Ok(pooled)
}

// ----------------------------------------------------------------------------
// data
// ----------------------------------------------------------------------------

/// Parsed NSDs.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Nsds(Vec<Nsd>);

impl Nsds {
    fn from_reader<Input: BufRead>(input: Input) -> Result<Self> {
        let mut index = Index::default();
        let mut nsds = Self::default();

        for line in input.lines() {
            let line = line?;

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[2] == "HEADER" {
                index = Index::default();
                header_to_index(&tokens, &mut index);
            } else {
                let entry = Nsd::from_tokens(&tokens, &index)?;
                nsds.0.push(entry);
            }
        }

        Ok(nsds)
    }

    /// Returns an [`Iterator`] over the NSDs.
    #[must_use]
    pub fn iter(&self) -> std::slice::Iter<Nsd> {
        self.0.iter()
    }

    /// Returns the underlying collection.
    // ALLOW false positive: constant functions cannot evaluate destructors
    #[allow(clippy::missing_const_for_fn)]
    #[must_use]
    pub fn into_inner(self) -> Vec<Nsd> {
        self.0
    }
}

impl IntoIterator for Nsds {
    type Item = Nsd;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// NSD data.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Nsd {
    name: String,
    server_list: Vec<String>,
    device: String,
}

impl Nsd {
    /// Returns the NSD name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the NSD server list.
    #[must_use]
    pub fn server_list(&self) -> &[String] {
        &self.server_list
    }

    /// Returns the local NSD name.
    #[must_use]
    pub fn device(&self) -> &str {
        &self.device
    }

    /// Returns the local device name.
    ///
    /// # Errors
    ///
    /// Returns an error if [`Path`] can't determine the file name for the
    /// device path.
    pub fn device_name(&self) -> Result<Cow<str>> {
        Path::new(&self.device)
            .file_name()
            .ok_or_else(|| {
                anyhow!("unable to get file name for device {}", self.device)
            })
            .map(OsStr::to_string_lossy)
    }
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl Nsd {
    fn from_tokens(tokens: &[&str], index: &Index) -> Result<Self> {
        let name_index =
            index.name.ok_or_else(|| anyhow!("no disk name index"))?;
        let name = tokens[name_index].into();

        let server_list_index = index
            .server_list
            .ok_or_else(|| anyhow!("no server list index"))?;
        let server_list = tokens[server_list_index];
        let server_list = server_list.split(',').map(Into::into).collect();

        let local_name_index = index
            .local_name
            .ok_or_else(|| anyhow!("no local disk name index"))?;
        let local_name = tokens[local_name_index].into();

        Ok(Self {
            name,
            server_list,
            device: local_name,
        })
    }
}

#[derive(Debug, Default)]
struct Index {
    name: Option<usize>,
    server_list: Option<usize>,
    local_name: Option<usize>,
}

fn header_to_index(tokens: &[&str], index: &mut Index) {
    for (i, token) in tokens.iter().enumerate() {
        match *token {
            "diskName" => index.name = Some(i),
            "serverList" => index.server_list = Some(i),
            "localDiskName" => index.local_name = Some(i),
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
        let input = include_str!("nsd-example.in");

        let fs = Nsds::from_reader(input.as_bytes()).unwrap();
        let mut fs = fs.0.into_iter();

        assert_eq!(
            fs.next(),
            Some(Nsd {
                name: "disk1".into(),
                server_list: vec!["filer1".into()],
                device: "/dev/dm-1".into(),
            })
        );

        assert_eq!(
            fs.next(),
            Some(Nsd {
                name: "disk2".into(),
                server_list: vec!["filer2".into()],
                device: "/dev/dm-2".into(),
            })
        );

        assert_eq!(
            fs.next(),
            Some(Nsd {
                name: "disk3".into(),
                server_list: vec!["filer3".into()],
                device: "/dev/dm-3".into(),
            })
        );

        assert_eq!(fs.next(), None);
    }
}
