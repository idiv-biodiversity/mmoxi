#![allow(dead_code)]

//! `mmdf` parsing.

use std::io::BufRead;
use std::process::Command;

use anyhow::{anyhow, Context, Result};

use crate::util::MMBool;

/// Runs `mmdf` on the given file system.
///
/// # Errors
///
/// Returns an error if running `mmdf` fails or if parsing its output fails.
pub fn run(fs: &str) -> Result<Data> {
    let mut cmd = Command::new("mmdf");
    cmd.arg(fs);
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {cmd:?}"))?;

    let data = Data::from_reader(fs, output.stdout.as_slice())?;

    Ok(data)
}

/// The data returned by `mmdf`.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Data {
    /// Returns the file system name.
    pub fs: String,

    /// Returns the NSDs.
    pub nsds: Vec<Nsd>,

    /// Returns the per pool totals.
    pub pools: Vec<Pool>,

    /// Returns the file system totals.
    pub total: Filesystem,
}

impl Data {
    fn new(fs: impl Into<String>) -> Self {
        Self {
            fs: fs.into(),
            nsds: vec![],
            pools: vec![],
            total: Filesystem::default(),
        }
    }
}

/// A filesystem total entry.
#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default,
)]
pub struct Filesystem {
    size: u64,
    free_blocks: u64,
    free_blocks_percent: u64,
    free_fragments: u64,
    free_fragments_percent: u64,
}

impl Filesystem {
    /// Returns the NSD size in kilobytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns the free blocks in kilobytes.
    #[must_use]
    pub const fn free_blocks(&self) -> u64 {
        self.free_blocks
    }

    /// Returns the free blocks in percent.
    #[must_use]
    pub const fn free_blocks_percent(&self) -> u64 {
        self.free_blocks_percent
    }

    /// Returns the free fragments in kilobytes.
    #[must_use]
    pub const fn free_fragments(&self) -> u64 {
        self.free_fragments
    }

    /// Returns the free fragments in percent.
    #[must_use]
    pub const fn free_fragments_percent(&self) -> u64 {
        self.free_fragments_percent
    }
}

/// A pool total entry.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Pool {
    name: String,
    size: u64,
    free_blocks: u64,
    free_blocks_percent: u64,
    free_fragments: u64,
    free_fragments_percent: u64,
}

impl Pool {
    /// Returns the pool name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Returns the NSD size in kilobytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns the free blocks in kilobytes.
    #[must_use]
    pub const fn free_blocks(&self) -> u64 {
        self.free_blocks
    }

    /// Returns the free blocks in percent.
    #[must_use]
    pub const fn free_blocks_percent(&self) -> u64 {
        self.free_blocks_percent
    }

    /// Returns the free fragments in kilobytes.
    #[must_use]
    pub const fn free_fragments(&self) -> u64 {
        self.free_fragments
    }

    /// Returns the free fragments in percent.
    #[must_use]
    pub const fn free_fragments_percent(&self) -> u64 {
        self.free_fragments_percent
    }
}

/// An NSD entry.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Nsd {
    name: String,
    pool: String,
    size: u64,
    metadata: bool,
    data: bool,
    free_blocks: u64,
    free_blocks_percent: u64,
    free_fragments: u64,
    free_fragments_percent: u64,
}

impl Nsd {
    /// Returns the NSD name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    /// Returns the pool name this NSD belongs to.
    #[must_use]
    pub fn pool(&self) -> &str {
        self.pool.as_ref()
    }

    /// Returns the NSD size in kilobytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns `true` if this NSD holds metadata.
    #[must_use]
    pub const fn holds_metadata(&self) -> bool {
        self.metadata
    }

    /// Returns `true` if this NSD holds object data.
    #[must_use]
    pub const fn holds_objectdata(&self) -> bool {
        self.data
    }

    /// Returns the free blocks in kilobytes.
    #[must_use]
    pub const fn free_blocks(&self) -> u64 {
        self.free_blocks
    }

    /// Returns the free blocks in percent.
    #[must_use]
    pub const fn free_blocks_percent(&self) -> u64 {
        self.free_blocks_percent
    }

    /// Returns the free fragments in kilobytes.
    #[must_use]
    pub const fn free_fragments(&self) -> u64 {
        self.free_fragments
    }

    /// Returns the free fragments in percent.
    #[must_use]
    pub const fn free_fragments_percent(&self) -> u64 {
        self.free_fragments_percent
    }
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl Data {
    fn from_reader<Input: BufRead>(fs: &str, input: Input) -> Result<Self> {
        let mut fs_index = FilesystemIndex::default();
        let mut nsd_index = NsdIndex::default();
        let mut pool_index = PoolIndex::default();

        let mut data = Self::new(fs);

        for line in input.lines() {
            let line = line?;

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[1] == "nsd" {
                if tokens[2] == "HEADER" {
                    nsd_index.with_tokens(&tokens);
                } else {
                    let entry = Nsd::from_tokens(&tokens, &nsd_index)?;
                    data.nsds.push(entry);
                }
            } else if tokens[1] == "poolTotal" {
                if tokens[2] == "HEADER" {
                    pool_index.with_tokens(&tokens);
                } else {
                    let entry = Pool::from_tokens(&tokens, &pool_index)?;
                    data.pools.push(entry);
                }
            } else if tokens[1] == "fsTotal" {
                if tokens[2] == "HEADER" {
                    fs_index.with_tokens(&tokens);
                } else {
                    let entry = Filesystem::from_tokens(&tokens, &fs_index)?;
                    data.total = entry;
                }
            }
        }

        Ok(data)
    }
}

#[derive(Debug, Default)]
struct FilesystemIndex {
    size: Option<usize>,
    free_blocks: Option<usize>,
    free_blocks_percent: Option<usize>,
    free_fragments: Option<usize>,
    free_fragments_percent: Option<usize>,
}

impl FilesystemIndex {
    fn with_tokens(&mut self, tokens: &[&str]) {
        for (i, token) in tokens.iter().enumerate() {
            match *token {
                "fsSize" => self.size = Some(i),
                "freeBlocks" => self.free_blocks = Some(i),
                "freeBlocksPct" => self.free_blocks_percent = Some(i),
                "freeFragments" => self.free_fragments = Some(i),
                "freeFragmentsPct" => self.free_fragments_percent = Some(i),
                _ => {}
            }
        }
    }
}

#[derive(Debug, Default)]
struct NsdIndex {
    name: Option<usize>,
    pool: Option<usize>,
    size: Option<usize>,
    metadata: Option<usize>,
    data: Option<usize>,
    free_blocks: Option<usize>,
    free_blocks_percent: Option<usize>,
    free_fragments: Option<usize>,
    free_fragments_percent: Option<usize>,
}

impl NsdIndex {
    fn with_tokens(&mut self, tokens: &[&str]) {
        for (i, token) in tokens.iter().enumerate() {
            match *token {
                "nsdName" => self.name = Some(i),
                "storagePool" => self.pool = Some(i),
                "diskSize" => self.size = Some(i),
                "metadata" => self.metadata = Some(i),
                "data" => self.data = Some(i),
                "freeBlocks" => self.free_blocks = Some(i),
                "freeBlocksPct" => self.free_blocks_percent = Some(i),
                "freeFragments" => self.free_fragments = Some(i),
                "freeFragmentsPct" => self.free_fragments_percent = Some(i),
                _ => {}
            }
        }
    }
}

#[derive(Debug, Default)]
struct PoolIndex {
    name: Option<usize>,
    size: Option<usize>,
    free_blocks: Option<usize>,
    free_blocks_percent: Option<usize>,
    free_fragments: Option<usize>,
    free_fragments_percent: Option<usize>,
}

impl PoolIndex {
    fn with_tokens(&mut self, tokens: &[&str]) {
        for (i, token) in tokens.iter().enumerate() {
            match *token {
                "poolName" => self.name = Some(i),
                "poolSize" => self.size = Some(i),
                "freeBlocks" => self.free_blocks = Some(i),
                "freeBlocksPct" => self.free_blocks_percent = Some(i),
                "freeFragments" => self.free_fragments = Some(i),
                "freeFragmentsPct" => self.free_fragments_percent = Some(i),
                _ => {}
            }
        }
    }
}

impl Filesystem {
    fn from_tokens(tokens: &[&str], index: &FilesystemIndex) -> Result<Self> {
        let size_index =
            index.size.ok_or_else(|| anyhow!("no fsSize index"))?;
        let size = tokens[size_index];
        let size = size
            .parse()
            .with_context(|| format!("invalid fsSize field: {size}"))?;

        let free_blocks_index = index
            .free_blocks
            .ok_or_else(|| anyhow!("no freeBlocks index"))?;
        let free_blocks = tokens[free_blocks_index];
        let free_blocks = free_blocks.parse().with_context(|| {
            format!("invalid freeBlocks field: {free_blocks}")
        })?;

        let free_blocks_percent_index = index
            .free_blocks_percent
            .ok_or_else(|| anyhow!("no freeBlocksPct index"))?;
        let free_blocks_percent = tokens[free_blocks_percent_index];
        let free_blocks_percent =
            free_blocks_percent.parse().with_context(|| {
                format!("invalid freeBlocksPct field: {free_blocks_percent}")
            })?;

        let free_fragments_index = index
            .free_fragments
            .ok_or_else(|| anyhow!("no freeFragments index"))?;
        let free_fragments = tokens[free_fragments_index];
        let free_fragments = free_fragments.parse().with_context(|| {
            format!("invalid freeFragments field: {free_fragments}")
        })?;

        let free_fragments_percent_index = index
            .free_fragments_percent
            .ok_or_else(|| anyhow!("no freeFragmentsPct index"))?;
        let free_fragments_percent = tokens[free_fragments_percent_index];
        let free_fragments_percent =
            free_fragments_percent.parse().with_context(|| {
                format!(
                    "invalid freeFragmentsPct field: {free_fragments_percent}"
                )
            })?;

        Ok(Self {
            size,
            free_blocks,
            free_blocks_percent,
            free_fragments,
            free_fragments_percent,
        })
    }
}

impl Nsd {
    fn from_tokens(tokens: &[&str], index: &NsdIndex) -> Result<Self> {
        let name_index =
            index.name.ok_or_else(|| anyhow!("no nsdName index"))?;
        let name = tokens[name_index].into();

        let storage_pool_index =
            index.pool.ok_or_else(|| anyhow!("no storagePool index"))?;
        let pool = tokens[storage_pool_index].into();

        let disk_size_index =
            index.size.ok_or_else(|| anyhow!("no diskSize index"))?;
        let size = tokens[disk_size_index];
        let size = size
            .parse()
            .with_context(|| format!("invalid diskSize field: {size}"))?;

        let metadata_index =
            index.metadata.ok_or_else(|| anyhow!("no metadata index"))?;
        let metadata = tokens[metadata_index];
        let metadata = metadata
            .parse::<MMBool>()
            .with_context(|| format!("invalid metadata field: {metadata}"))?
            .as_bool();

        let data_index = index.data.ok_or_else(|| anyhow!("no data index"))?;
        let data = tokens[data_index];
        let data = data
            .parse::<MMBool>()
            .with_context(|| format!("invalid data field: {data}"))?
            .as_bool();

        let free_blocks_index = index
            .free_blocks
            .ok_or_else(|| anyhow!("no freeBlocks index"))?;
        let free_blocks = tokens[free_blocks_index];
        let free_blocks = free_blocks.parse().with_context(|| {
            format!("invalid freeBlocks field: {free_blocks}")
        })?;

        let free_blocks_percent_index = index
            .free_blocks_percent
            .ok_or_else(|| anyhow!("no freeBlocksPct index"))?;
        let free_blocks_percent = tokens[free_blocks_percent_index];
        let free_blocks_percent =
            free_blocks_percent.parse().with_context(|| {
                format!("invalid freeBlocksPct field: {free_blocks_percent}")
            })?;

        let free_fragments_index = index
            .free_fragments
            .ok_or_else(|| anyhow!("no freeFragments index"))?;
        let free_fragments = tokens[free_fragments_index];
        let free_fragments = free_fragments.parse().with_context(|| {
            format!("invalid freeFragments field: {free_fragments}")
        })?;

        let free_fragments_percent_index = index
            .free_fragments_percent
            .ok_or_else(|| anyhow!("no freeFragmentsPct index"))?;
        let free_fragments_percent = tokens[free_fragments_percent_index];
        let free_fragments_percent =
            free_fragments_percent.parse().with_context(|| {
                format!(
                    "invalid freeFragmentsPct field: {free_fragments_percent}"
                )
            })?;

        Ok(Self {
            name,
            pool,
            size,
            metadata,
            data,
            free_blocks,
            free_blocks_percent,
            free_fragments,
            free_fragments_percent,
        })
    }
}

impl Pool {
    fn from_tokens(tokens: &[&str], index: &PoolIndex) -> Result<Self> {
        let name_index =
            index.name.ok_or_else(|| anyhow!("no poolName index"))?;
        let name = tokens[name_index].into();

        let size_index =
            index.size.ok_or_else(|| anyhow!("no poolSize index"))?;
        let size = tokens[size_index];
        let size = size
            .parse()
            .with_context(|| format!("invalid poolSize field: {size}"))?;

        let free_blocks_index = index
            .free_blocks
            .ok_or_else(|| anyhow!("no freeBlocks index"))?;
        let free_blocks = tokens[free_blocks_index];
        let free_blocks = free_blocks.parse().with_context(|| {
            format!("invalid freeBlocks field: {free_blocks}")
        })?;

        let free_blocks_percent_index = index
            .free_blocks_percent
            .ok_or_else(|| anyhow!("no freeBlocksPct index"))?;
        let free_blocks_percent = tokens[free_blocks_percent_index];
        let free_blocks_percent =
            free_blocks_percent.parse().with_context(|| {
                format!("invalid freeBlocksPct field: {free_blocks_percent}")
            })?;

        let free_fragments_index = index
            .free_fragments
            .ok_or_else(|| anyhow!("no freeFragments index"))?;
        let free_fragments = tokens[free_fragments_index];
        let free_fragments = free_fragments.parse().with_context(|| {
            format!("invalid freeFragments field: {free_fragments}")
        })?;

        let free_fragments_percent_index = index
            .free_fragments_percent
            .ok_or_else(|| anyhow!("no freeFragmentsPct index"))?;
        let free_fragments_percent = tokens[free_fragments_percent_index];
        let free_fragments_percent =
            free_fragments_percent.parse().with_context(|| {
                format!(
                    "invalid freeFragmentsPct field: {free_fragments_percent}"
                )
            })?;

        Ok(Self {
            name,
            size,
            free_blocks,
            free_blocks_percent,
            free_fragments,
            free_fragments_percent,
        })
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
        let input = include_str!("df-example.in");

        let Data {
            fs: _,
            nsds,
            pools,
            total,
        } = Data::from_reader("gpfs1", input.as_bytes()).unwrap();

        let mut nsds = nsds.into_iter();

        assert_eq!(
            nsds.next(),
            Some(Nsd {
                name: "filer3_nvme02".into(),
                pool: "nvme".into(),
                size: 6_251_223_376,
                metadata: false,
                data: true,
                free_blocks: 2_703_523_840,
                free_blocks_percent: 43,
                free_fragments: 621_356_336,
                free_fragments_percent: 10,
            })
        );

        assert_eq!(
            nsds.next(),
            Some(Nsd {
                name: "filer3_nvme03".into(),
                pool: "nvme".into(),
                size: 6_251_223_376,
                metadata: false,
                data: true,
                free_blocks: 2_696_495_104,
                free_blocks_percent: 43,
                free_fragments: 621_595_888,
                free_fragments_percent: 10,
            })
        );

        assert_eq!(
            nsds.next(),
            Some(Nsd {
                name: "filer3_nvme04".into(),
                pool: "nvme".into(),
                size: 6_251_223_376,
                metadata: false,
                data: true,
                free_blocks: 2_703_433_728,
                free_blocks_percent: 43,
                free_fragments: 618_577_952,
                free_fragments_percent: 10,
            })
        );

        assert_eq!(nsds.next(), None);

        let mut pools = pools.into_iter();

        assert_eq!(
            pools.next(),
            Some(Pool {
                name: "system".into(),
                size: 50_009_787_008,
                free_blocks: 34_200_018_944,
                free_blocks_percent: 68,
                free_fragments: 1_056_310_560,
                free_fragments_percent: 2,
            })
        );

        assert_eq!(
            pools.next(),
            Some(Pool {
                name: "nvme".into(),
                size: 350_068_509_056,
                free_blocks: 98_512_142_336,
                free_blocks_percent: 28,
                free_fragments: 39_131_446_656,
                free_fragments_percent: 11,
            })
        );

        assert_eq!(pools.next(), None);

        assert_eq!(
            total,
            Filesystem {
                size: 5_055_008_965_696,
                free_blocks: 2_115_551_961_088,
                free_blocks_percent: 42,
                free_fragments: 92_600_022_848,
                free_fragments_percent: 2,
            }
        );
    }
}
