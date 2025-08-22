//! Pool-based user distribution.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::ops::AddAssign;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{anyhow, Context, Result};
use bstr::io::BufReadExt;
use bstr::ByteSlice;
use tempfile::{tempdir, tempdir_in};

/// Runs `mmapplypolicy` on a file system pool to find out how much file sizes
/// users have.
///
/// # Errors
///
/// - creating tmp directory
/// - writing policy file
/// - running `mmapplypolicy`
/// - parsing `mmapplypolicy` output
pub fn run(
    device_or_dir: impl Into<String>,
    pool: impl Into<String>,
    fileset: Option<impl AsRef<str>>,
    nodes: Option<impl AsRef<OsStr>>,
    local_work_dir: Option<impl AsRef<Path>>,
    global_work_dir: Option<impl AsRef<Path>>,
    scope: Option<impl AsRef<str>>,
) -> Result<Data> {
    let device_or_dir = device_or_dir.into();
    let pool = pool.into();

    let tmp = if let Some(ref local_work_dir) = local_work_dir {
        tempdir_in(local_work_dir)?
    } else {
        tempdir()?
    };

    let policy = tmp.path().join(".policy");
    let prefix = tmp.path().join("pool-scanner");

    let mut file = File::create(&policy)?;
    write_policy(&mut file, &pool, fileset)?;
    file.sync_all()?;

    let mut command = Command::new("mmapplypolicy");
    command
        .arg(&device_or_dir)
        .args([OsStr::new("-P"), policy.as_os_str()])
        .args([OsStr::new("-f"), prefix.as_os_str()])
        .args(["--choice-algorithm", "fast"])
        .args(["-I", "defer"])
        .args(["-L", "0"]);

    if let Some(nodes) = nodes {
        command.arg("-N").arg(nodes.as_ref());
    }

    if let Some(local_work_dir) = local_work_dir {
        command.arg("-s").arg(local_work_dir.as_ref());
    }

    if let Some(global_work_dir) = global_work_dir {
        command.arg("-g").arg(global_work_dir.as_ref());
    }

    if let Some(scope) = scope {
        command.arg("--scope").arg(scope.as_ref());
    }

    let mut child = command.stdout(Stdio::null()).spawn().context(
        "mmapplypolicy failed to start, make sure it's on your PATH",
    )?;

    let ecode = child
        .wait()
        .with_context(|| "failed waiting on mmapplypolicy")?;

    if !ecode.success() {
        return Err(anyhow!(
            "mmapplypolicy was no success, exit code: {ecode}"
        ));
    }

    let list = tmp.path().join("pool-scanner.list.users");
    let list = File::open(&list).with_context(|| {
        format!("failed to open policy output: {}", list.display())
    })?;
    let list = BufReader::new(list);

    let mut raw = sum(list)?;
    let mut named_user_sizes = HashMap::with_capacity(raw.len());

    for (user, data) in raw.drain() {
        let user = crate::user::by_uid(&user).unwrap_or(user);
        named_user_sizes.insert(user, data);
    }

    let data = Data {
        device_or_dir,
        pool,
        raw: named_user_sizes,
    };

    Ok(data)
}

/// The data structure returned by this module.
pub struct Data {
    device_or_dir: String,
    pool: String,
    raw: HashMap<String, Summary>,
}

impl crate::prom::ToText for Data {
    fn to_prom(&self, output: &mut impl Write) -> Result<()> {
        let device_or_dir = &self.device_or_dir;
        let pool = &self.pool;

        writeln!(
            output,
            "# HELP gpfs_pool_user_distribution_files GPFS pool files per user"
        )?;

        writeln!(output, "# TYPE gpfs_pool_user_distribution_files gauge")?;

        for (user, data) in &self.raw {
            writeln!(
                output,
                "gpfs_pool_user_distribution_files{{device_or_dir=\"{}\",pool=\"{}\",user=\"{}\"}} {}",
                device_or_dir,
                pool,
                user,
                data.files,
            )?;
        }

        writeln!(
            output,
            "# HELP gpfs_pool_user_distribution_file_size GPFS pool file size per user in bytes"
        )?;

        writeln!(
            output,
            "# TYPE gpfs_pool_user_distribution_file_size gauge"
        )?;

        for (user, data) in &self.raw {
            writeln!(
                output,
                "gpfs_pool_user_distribution_file_size{{device_or_dir=\"{}\",pool=\"{}\",user=\"{}\"}} {}",
                device_or_dir,
                pool,
                user,
                data.file_size,
            )?;
        }

        writeln!(
            output,
            "# HELP gpfs_pool_user_distribution_allocated GPFS pool allocated storage per user in kilobytes"
        )?;

        writeln!(
            output,
            "# TYPE gpfs_pool_user_distribution_allocated gauge"
        )?;

        for (user, data) in &self.raw {
            writeln!(
                output,
                "gpfs_pool_user_distribution_allocated{{device_or_dir=\"{}\",pool=\"{}\",user=\"{}\"}} {}",
                device_or_dir,
                pool,
                user,
                data.kb_allocated,
            )?;
        }

        Ok(())
    }
}

/// Data collected via policy.
#[derive(
    Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default,
)]
pub struct Summary {
    /// Returns the number of files.
    pub files: u64,

    /// Returns the file size in bytes.
    pub file_size: u64,

    /// Returns the allocated disk space in kilobytes.
    pub kb_allocated: u64,
}

impl AddAssign for Summary {
    fn add_assign(&mut self, rhs: Self) {
        self.files += rhs.files;
        self.file_size += rhs.file_size;
        self.kb_allocated += rhs.kb_allocated;
    }
}

fn sum<I>(input: I) -> Result<HashMap<String, Summary>>
where
    I: BufRead,
{
    let mut user_sizes: HashMap<String, Summary> = HashMap::default();

    for line in input.byte_lines() {
        let line = line?;

        let mut payload = line
            .splitn_str(6, " ")
            .nth(4)
            .context("no payload field")?
            .splitn_str(3, ":");

        let user = payload.next().context("no USER_ID field in payload")?;
        let user = user
            .to_str()
            .with_context(|| format!("not UTF-8: {user:?}"))?;

        let file_size =
            payload.next().context("no FILE_SIZE field in payload")?;
        let file_size = file_size
            .to_str()
            .with_context(|| format!("not UTF-8: {file_size:?}"))?;
        let file_size: u64 = file_size
            .parse()
            .with_context(|| format!("not a number: {file_size}"))?;

        let kb_allocated =
            payload.next().context("no KB_ALLOCATED field in payload")?;
        let kb_allocated = kb_allocated
            .to_str()
            .with_context(|| format!("not UTF-8: {kb_allocated:?}"))?;
        let kb_allocated: u64 = kb_allocated
            .parse()
            .with_context(|| format!("not a number: {kb_allocated}"))?;

        *user_sizes.entry(user.into()).or_default() += Summary {
            files: 1,
            file_size,
            kb_allocated,
        };
    }

    Ok(user_sizes)
}

fn write_policy(
    mut w: impl io::Write,
    pool: impl AsRef<str>,
    fileset: Option<impl AsRef<str>>,
) -> io::Result<()> {
    write!(
        w,
        "
RULE EXTERNAL LIST 'users' EXEC ''

RULE
  LIST 'users'
    FROM POOL '{}'",
        pool.as_ref()
    )?;

    if let Some(fileset) = fileset {
        write!(
            w,
            "
    FOR FILESET ('{}')",
            fileset.as_ref()
        )?;
    }

    write!(
        w,
        "
    WEIGHT(0)
    SHOW(VARCHAR(USER_ID) || ':' || VARCHAR(FILE_SIZE) || ':' || VARCHAR(KB_ALLOCATED))
",
    )?;

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
        let input = include_str!("pool_user_distribution_example.in");

        let user_sizes = sum(input.as_bytes()).unwrap();
        let mut user_sizes = user_sizes.into_iter().collect::<Vec<_>>();
        user_sizes.sort_unstable();
        let mut user_sizes = user_sizes.into_iter();

        assert_eq!(
            user_sizes.next(),
            Some((
                "1000".into(),
                Summary {
                    files: 6,
                    file_size: 322_255,
                    kb_allocated: 640,
                }
            ))
        );

        assert_eq!(
            user_sizes.next(),
            Some((
                "1001".into(),
                Summary {
                    files: 6,
                    file_size: 455_067,
                    kb_allocated: 960,
                }
            ))
        );

        assert_eq!(user_sizes.next(), None);
    }
}
