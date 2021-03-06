//! Prometheus metrics.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, Result};

use crate::nsd::FsPoolId;
use crate::sysfs;

/// Returns block device metrics grouped by pool.
///
/// # Errors
///
/// Returns an error if fetching the NSDs or block device statistics fails.
pub fn pool_block_device_metrics<Cache>(
    device_cache: Cache,
    force: bool,
) -> Result<PoolBlockDeviceMetrics>
where
    Cache: AsRef<Path>,
{
    let pooled_nsds = crate::nsd::local_pooled(device_cache, force)?;

    let stats = sysfs::block::stat_all()?;

    let mut metrics = PoolBlockDeviceMetrics::default();

    for (id, nsds) in pooled_nsds {
        let mut pool_stat_sum = sysfs::block::Stat::default();

        for nsd in nsds {
            let device_name = nsd.device_name()?;

            let stat = stats.get(device_name.as_ref()).ok_or_else(|| {
                anyhow!("no block device stat for device: {}", nsd.device())
            })?;

            pool_stat_sum += *stat;
        }

        metrics.inner.insert(id, pool_stat_sum);
    }

    Ok(metrics)
}

/// Block device metrics grouped by pool.
#[derive(Default)]
pub struct PoolBlockDeviceMetrics {
    inner: HashMap<FsPoolId, sysfs::block::Stat>,
}

impl PoolBlockDeviceMetrics {
    /// Writes data as prometheus metrics to `output`.
    ///
    /// # Errors
    ///
    /// This function uses [`writeln`] to write to `output`. It can only fail
    /// if any of these [`writeln`] fails.
    pub fn to_prom<Output: Write>(&self, output: &mut Output) -> Result<()> {
        writeln!(
            output,
            "# HELP gpfs_pool_read_ios GPFS pool processed read I/Os"
        )?;
        writeln!(output, "# TYPE gpfs_pool_read_ios counter")?;

        for (id, stat) in &self.inner {
            writeln!(
                output,
                "gpfs_pool_read_ios{{fs=\"{}\",pool=\"{}\"}} {}",
                id.fs(),
                id.pool(),
                stat.read_ios
            )?;
        }

        writeln!(output, "# HELP gpfs_pool_read_bytes GPFS pool read bytes")?;
        writeln!(output, "# TYPE gpfs_pool_read_bytes counter")?;

        for (id, stat) in &self.inner {
            writeln!(
                output,
                "gpfs_pool_read_bytes{{fs=\"{}\",pool=\"{}\"}} {}",
                id.fs(),
                id.pool(),
                stat.read_bytes()
            )?;
        }

        writeln!(
            output,
            "# HELP gpfs_pool_write_ios GPFS pool processed write I/Os"
        )?;
        writeln!(output, "# TYPE gpfs_pool_write_ios counter")?;

        for (id, stat) in &self.inner {
            writeln!(
                output,
                "gpfs_pool_write_ios{{fs=\"{}\",pool=\"{}\"}} {}",
                id.fs(),
                id.pool(),
                stat.write_ios
            )?;
        }

        writeln!(
            output,
            "# HELP gpfs_pool_write_bytes GPFS pool written bytes"
        )?;
        writeln!(output, "# TYPE gpfs_pool_write_bytes counter")?;

        for (id, stat) in &self.inner {
            writeln!(
                output,
                "gpfs_pool_write_bytes{{fs=\"{}\",pool=\"{}\"}} {}",
                id.fs(),
                id.pool(),
                stat.write_bytes()
            )?;
        }

        Ok(())
    }
}
