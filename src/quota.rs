//! `mmrepquota` parsing.
//!
//! # Examples
//!
//! ```no_run
//! use std::io::{self, BufWriter};
//!
//! use mmoxi::quota::Data;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let data = Data::from_reader(io::stdin().lock())?;
//!
//! let mut output = BufWriter::new(io::stdout());
//! data.to_prom(&mut output)?;
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::io::{BufRead, Write};
use std::str::FromStr;

use anyhow::{anyhow, Context, Result};

/// Parsed quota entries.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Data {
    entries: Vec<Entry>,
}

impl Data {
    /// Parse data from `mmrepquota` output.
    ///
    /// # Errors
    ///
    /// Reading from input or parsing it.
    pub fn from_reader<Input: BufRead>(input: Input) -> Result<Self> {
        let mut index = Index::default();
        let mut data = Self::default();

        for line in input.lines() {
            let line = line?;

            if line.starts_with("***") {
                continue;
            }

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[2] == "HEADER" {
                index = Index::default();
                header_to_index(&tokens, &mut index);
            } else {
                let entry = Entry::from_tokens(&tokens, &index)?;
                data.entries.push(entry);
            }
        }

        Ok(data)
    }

    /// Returns the entries.
    #[must_use]
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Writes all entries as prometheus metrics to `output`.
    ///
    /// # Errors
    ///
    /// This function uses [`writeln`] to write to `output`. It can only fail
    /// if any of these [`writeln`] fails.
    pub fn to_prom<Output: Write>(&self, output: &mut Output) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        prom_block_usage(&self.entries, output)?;
        prom_block_quota(&self.entries, output)?;
        prom_block_limit(&self.entries, output)?;
        prom_block_in_doubt(&self.entries, output)?;

        prom_files_usage(&self.entries, output)?;
        prom_files_quota(&self.entries, output)?;
        prom_files_limit(&self.entries, output)?;
        prom_files_in_doubt(&self.entries, output)?;

        Ok(())
    }
}

/// Parsed quota entry.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Entry {
    fs_name: String,
    quota_type: Type,
    id: u64,
    name: String,
    block: Metrics,
    files: Metrics,
    fileset_name: String,
}

impl Entry {
    /// Returns the file system name.
    #[must_use]
    pub fn fs_name(&self) -> &str {
        &self.fs_name
    }

    /// Returns the quota type.
    #[must_use]
    pub const fn quota_type(&self) -> Type {
        self.quota_type
    }

    /// Returns the user/group/fileset id (depending on [`Type`]).
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Returns the user/group/fileset name (depending on [`Type`]).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the block quota.
    #[must_use]
    pub const fn block(&self) -> Metrics {
        self.block
    }

    /// Returns the files quota.
    #[must_use]
    pub const fn files(&self) -> Metrics {
        self.files
    }

    /// Returns the name of the fileset that contains this entry.
    #[must_use]
    pub fn fileset_name(&self) -> &str {
        &self.fileset_name
    }
}

/// Quota metrics. The returned values are in kilobytes for block quotas and in
/// number of files for file quotas.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default,
)]
pub struct Metrics {
    usage: i64,
    quota: u64,
    limit: u64,
    in_doubt: u64,
}

impl Metrics {
    /// Returns the current usage.
    #[must_use]
    pub const fn usage(&self) -> i64 {
        self.usage
    }

    /// Returns the soft quota limit.
    #[must_use]
    pub const fn quota(&self) -> u64 {
        self.quota
    }

    /// Returns the hard quota limit.
    #[must_use]
    pub const fn limit(&self) -> u64 {
        self.limit
    }

    /// Returns the in doubt amount.
    #[must_use]
    pub const fn in_doubt(&self) -> u64 {
        self.in_doubt
    }
}

/// Quota type.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Type {
    /// Fileset quota.
    Fileset,

    /// Group quota.
    Group,

    /// User quota.
    User,
}

impl FromStr for Type {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "FILESET" => Ok(Self::Fileset),
            "GRP" => Ok(Self::Group),
            "USR" => Ok(Self::User),
            unknown => Err(anyhow!("unknown quota type: {}", unknown)),
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let r = match self {
            Self::Fileset => "FILESET",
            Self::Group => "GRP",
            Self::User => "USR",
        };

        write!(f, "{r}")
    }
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl Entry {
    fn from_tokens(tokens: &[&str], index: &Index) -> Result<Self> {
        let fs_name_index =
            index.fs_name.ok_or(anyhow!("no filesystem name index"))?;
        let fs_name = tokens[fs_name_index].into();

        let quota_type_index =
            index.quota_type.ok_or(anyhow!("no quota type index"))?;
        let quota_type = tokens[quota_type_index].parse::<Type>()?;

        let id_index = index.id.ok_or(anyhow!("no id index"))?;
        let id = tokens[id_index];
        let id = id.parse().with_context(|| format!("invalid id: {id}"))?;

        let name_index = index.name.ok_or(anyhow!("no name index"))?;
        let name = tokens[name_index].into();

        let block_usage_index =
            index.block_usage.ok_or(anyhow!("no block usage index"))?;
        let block_usage = tokens[block_usage_index];
        let block_usage = block_usage
            .parse()
            .with_context(|| format!("invalid block usage: {block_usage}"))?;

        let block_quota_index =
            index.block_quota.ok_or(anyhow!("no block quota index"))?;
        let block_quota = tokens[block_quota_index];
        let block_quota = block_quota
            .parse()
            .with_context(|| format!("invalid block quota: {block_quota}"))?;

        let block_limit_index =
            index.block_limit.ok_or(anyhow!("no block limit index"))?;
        let block_limit = tokens[block_limit_index];
        let block_limit = block_limit
            .parse()
            .with_context(|| format!("invalid block limit: {block_limit}"))?;

        let block_in_doubt_index = index
            .block_in_doubt
            .ok_or(anyhow!("no block in doubt index"))?;
        let block_in_doubt = tokens[block_in_doubt_index];
        let block_in_doubt = block_in_doubt.parse().with_context(|| {
            format!("invalid block in doubt: {block_in_doubt}")
        })?;

        let files_usage_index =
            index.files_usage.ok_or(anyhow!("no files usage index"))?;
        let files_usage = tokens[files_usage_index];
        let files_usage = files_usage
            .parse()
            .with_context(|| format!("invalid files usage: {files_usage}"))?;

        let files_quota_index =
            index.files_quota.ok_or(anyhow!("no files quota index"))?;
        let files_quota = tokens[files_quota_index];
        let files_quota = files_quota
            .parse()
            .with_context(|| format!("invalid files quota: {files_quota}"))?;

        let files_limit_index =
            index.files_limit.ok_or(anyhow!("no files limit index"))?;
        let files_limit = tokens[files_limit_index];
        let files_limit = files_limit
            .parse()
            .with_context(|| format!("invalid files limit: {files_limit}"))?;

        let files_in_doubt_index = index
            .files_in_doubt
            .ok_or(anyhow!("no files in doubt index"))?;
        let files_in_doubt = tokens[files_in_doubt_index];
        let files_in_doubt = files_in_doubt.parse().with_context(|| {
            format!("invalid files in doubt: {files_in_doubt}")
        })?;

        let fileset_name_index =
            index.fileset_name.ok_or(anyhow!("no fileset name index"))?;
        let fileset_name = tokens[fileset_name_index].into();

        Ok(Self {
            fs_name,
            quota_type,
            id,
            name,
            block: Metrics {
                usage: block_usage,
                quota: block_quota,
                limit: block_limit,
                in_doubt: block_in_doubt,
            },
            files: Metrics {
                usage: files_usage,
                quota: files_quota,
                limit: files_limit,
                in_doubt: files_in_doubt,
            },
            fileset_name,
        })
    }
}

#[derive(Debug, Default)]
struct Index {
    fs_name: Option<usize>,
    quota_type: Option<usize>,
    id: Option<usize>,
    name: Option<usize>,
    block_usage: Option<usize>,
    block_quota: Option<usize>,
    block_limit: Option<usize>,
    block_in_doubt: Option<usize>,
    files_usage: Option<usize>,
    files_quota: Option<usize>,
    files_limit: Option<usize>,
    files_in_doubt: Option<usize>,
    fileset_name: Option<usize>,
}

fn header_to_index(tokens: &[&str], index: &mut Index) {
    for (i, token) in tokens.iter().enumerate() {
        match *token {
            "filesystemName" => index.fs_name = Some(i),
            "quotaType" => index.quota_type = Some(i),
            "id" => index.id = Some(i),
            "name" => index.name = Some(i),
            "blockUsage" => index.block_usage = Some(i),
            "blockQuota" => index.block_quota = Some(i),
            "blockLimit" => index.block_limit = Some(i),
            "blockInDoubt" => index.block_in_doubt = Some(i),
            "filesUsage" => index.files_usage = Some(i),
            "filesQuota" => index.files_quota = Some(i),
            "filesLimit" => index.files_limit = Some(i),
            "filesInDoubt" => index.files_in_doubt = Some(i),
            "filesetname" => index.fileset_name = Some(i),
            _ => {}
        }
    }
}

// ----------------------------------------------------------------------------
// boiler-platy prometheus output
// ----------------------------------------------------------------------------

fn prom_block_usage<O: Write>(data: &[Entry], output: &mut O) -> Result<()> {
    writeln!(
        output,
        "# HELP gpfs_quota_block_usage_kbytes GPFS quota block usage in kilobytes."
    )?;
    writeln!(output, "# TYPE gpfs_quota_block_usage_kbytes gauge")?;

    for data in data {
        writeln!(
            output,
            "gpfs_quota_block_usage_kbytes{{fs=\"{}\",type=\"{}\",id=\"{}\",name=\"{}\",fileset=\"{}\"}} {}",
            data.fs_name,
            data.quota_type,
            data.id,
            data.name,
            data.fileset_name,
            data.block.usage,
        )?;
    }

    Ok(())
}

fn prom_block_quota<O: Write>(data: &[Entry], output: &mut O) -> Result<()> {
    writeln!(
        output,
        "# HELP gpfs_quota_block_quota_kbytes GPFS quota block quota in kilobytes."
    )?;
    writeln!(output, "# TYPE gpfs_quota_block_quota_kbytes gauge")?;

    for data in data {
        writeln!(
            output,
            "gpfs_quota_block_quota_kbytes{{fs=\"{}\",type=\"{}\",id=\"{}\",name=\"{}\",fileset=\"{}\"}} {}",
            data.fs_name,
            data.quota_type,
            data.id,
            data.name,
            data.fileset_name,
            data.block.quota,
        )?;
    }

    Ok(())
}

fn prom_block_limit<O: Write>(data: &[Entry], output: &mut O) -> Result<()> {
    writeln!(
        output,
        "# HELP gpfs_quota_block_limit_kbytes GPFS quota block limit in kilobytes."
    )?;
    writeln!(output, "# TYPE gpfs_quota_block_limit_kbytes gauge")?;

    for data in data {
        writeln!(
            output,
            "gpfs_quota_block_limit_kbytes{{fs=\"{}\",type=\"{}\",id=\"{}\",name=\"{}\",fileset=\"{}\"}} {}",
            data.fs_name,
            data.quota_type,
            data.id,
            data.name,
            data.fileset_name,
            data.block.limit,
        )?;
    }

    Ok(())
}

fn prom_block_in_doubt<O: Write>(
    data: &[Entry],
    output: &mut O,
) -> Result<()> {
    writeln!(
        output,
        "# HELP gpfs_quota_block_in_doubt_kbytes GPFS quota block in doubt in kilobytes."
    )?;
    writeln!(output, "# TYPE gpfs_quota_block_in_doubt_kbytes gauge")?;

    for data in data {
        writeln!(
            output,
            "gpfs_quota_block_in_doubt_kbytes{{fs=\"{}\",type=\"{}\",id=\"{}\",name=\"{}\",fileset=\"{}\"}} {}",
            data.fs_name,
            data.quota_type,
            data.id,
            data.name,
            data.fileset_name,
            data.block.in_doubt,
        )?;
    }

    Ok(())
}

fn prom_files_usage<O: Write>(data: &[Entry], output: &mut O) -> Result<()> {
    writeln!(
        output,
        "# HELP gpfs_quota_files_usage_kbytes GPFS quota block usage in kilobytes."
    )?;
    writeln!(output, "# TYPE gpfs_quota_files_usage_kbytes gauge")?;

    for data in data {
        writeln!(
            output,
            "gpfs_quota_files_usage_kbytes{{fs=\"{}\",type=\"{}\",id=\"{}\",name=\"{}\",fileset=\"{}\"}} {}",
            data.fs_name,
            data.quota_type,
            data.id,
            data.name,
            data.fileset_name,
            data.files.usage,
        )?;
    }

    Ok(())
}

fn prom_files_quota<O: Write>(data: &[Entry], output: &mut O) -> Result<()> {
    writeln!(
        output,
        "# HELP gpfs_quota_files_quota_kbytes GPFS quota block quota in kilobytes."
    )?;
    writeln!(output, "# TYPE gpfs_quota_files_quota_kbytes gauge")?;

    for data in data {
        writeln!(
            output,
            "gpfs_quota_files_quota_kbytes{{fs=\"{}\",type=\"{}\",id=\"{}\",name=\"{}\",fileset=\"{}\"}} {}",
            data.fs_name,
            data.quota_type,
            data.id,
            data.name,
            data.fileset_name,
            data.files.quota,
        )?;
    }

    Ok(())
}

fn prom_files_limit<O: Write>(data: &[Entry], output: &mut O) -> Result<()> {
    writeln!(
        output,
        "# HELP gpfs_quota_files_limit_kbytes GPFS quota block limit in kilobytes."
    )?;
    writeln!(output, "# TYPE gpfs_quota_files_limit_kbytes gauge")?;

    for data in data {
        writeln!(
            output,
            "gpfs_quota_files_limit_kbytes{{fs=\"{}\",type=\"{}\",id=\"{}\",name=\"{}\",fileset=\"{}\"}} {}",
            data.fs_name,
            data.quota_type,
            data.id,
            data.name,
            data.fileset_name,
            data.files.limit,
        )?;
    }

    Ok(())
}

fn prom_files_in_doubt<O: Write>(
    data: &[Entry],
    output: &mut O,
) -> Result<()> {
    writeln!(
        output,
        "# HELP gpfs_quota_files_in_doubt_kbytes GPFS quota block in doubt in kilobytes."
    )?;
    writeln!(output, "# TYPE gpfs_quota_files_in_doubt_kbytes gauge")?;

    for data in data {
        writeln!(
            output,
            "gpfs_quota_files_in_doubt_kbytes{{fs=\"{}\",type=\"{}\",id=\"{}\",name=\"{}\",fileset=\"{}\"}} {}",
            data.fs_name,
            data.quota_type,
            data.id,
            data.name,
            data.fileset_name,
            data.files.in_doubt,
        )?;
    }

    Ok(())
}

// ----------------------------------------------------------------------------
// tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let input = include_str!("quota-example.in");
        let parsed = Data::from_reader(input.as_bytes()).unwrap();

        let expected = Data {
            entries: vec![
                Entry {
                    fs_name: "gpfs1".into(),
                    quota_type: Type::Fileset,
                    id: 1,
                    name: "name1".into(),
                    block: Metrics {
                        usage: 950_235_440,
                        quota: 4_294_967_296,
                        limit: 5_368_709_120,
                        in_doubt: 406_372_448,
                    },
                    files: Metrics {
                        usage: 553_624,
                        quota: 5_000_000,
                        limit: 20_000_000,
                        in_doubt: 0,
                    },
                    fileset_name: "".into(),
                },
                Entry {
                    fs_name: "gpfs1".into(),
                    quota_type: Type::User,
                    id: 62347,
                    name: "62347".into(),
                    block: Metrics {
                        usage: 455_894_288,
                        quota: 0,
                        limit: 0,
                        in_doubt: 0,
                    },
                    files: Metrics {
                        usage: 25_738,
                        quota: 0,
                        limit: 0,
                        in_doubt: 0,
                    },
                    fileset_name: "fileset1".into(),
                },
            ],
        };

        assert_eq!(parsed, expected);
    }

    #[test]
    fn prometheus() {
        let data = Data {
            entries: vec![
                Entry {
                    fs_name: "gpfs1".into(),
                    quota_type: Type::Fileset,
                    id: 1,
                    name: "name1".into(),
                    block: Metrics {
                        usage: 950_235_440,
                        quota: 4_294_967_296,
                        limit: 5_368_709_120,
                        in_doubt: 406_372_448,
                    },
                    files: Metrics {
                        usage: 553_624,
                        quota: 5_000_000,
                        limit: 20_000_000,
                        in_doubt: 0,
                    },
                    fileset_name: "".into(),
                },
                Entry {
                    fs_name: "gpfs1".into(),
                    quota_type: Type::User,
                    id: 62347,
                    name: "62347".into(),
                    block: Metrics {
                        usage: 455_894_288,
                        quota: 0,
                        limit: 0,
                        in_doubt: 0,
                    },
                    files: Metrics {
                        usage: 25_738,
                        quota: 0,
                        limit: 0,
                        in_doubt: 0,
                    },
                    fileset_name: "fileset1".into(),
                },
            ],
        };

        let mut output = vec![];
        data.to_prom(&mut output).unwrap();

        let result = std::str::from_utf8(output.as_slice()).unwrap();

        let expected = include_str!("quota-example.prom");
        assert_eq!(result, expected);
    }
}
