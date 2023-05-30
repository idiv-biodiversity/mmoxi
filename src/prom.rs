//! Prometheus metrics.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, Result};

use crate::fileset::Fileset;
use crate::nsd::FsPoolId;
use crate::sysfs;

/// Writes the `mmdf` NSD data as prometheus metrics to `output`.
///
/// # Errors
///
/// This function uses [`writeln`] to write to `output`. It can only fail if
/// any of these [`writeln`] fails.
pub fn write_df_nsd_metrics<H, O>(
    nsds: &HashMap<String, Vec<crate::df::Nsd>, H>,
    output: &mut O,
) -> Result<()>
where
    H: std::hash::BuildHasher,
    O: Write,
{
    writeln!(
        output,
        "# HELP gpfs_df_nsd_size GPFS mmdf NSD size in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_nsd_size gauge")?;

    for (fs, nsds) in nsds {
        for nsd in nsds {
            writeln!(
                output,
                "gpfs_df_nsd_size{{name=\"{}\",fs=\"{fs}\",pool=\"{}\",metadata=\"{}\",data=\"{}\"}} {}",
                nsd.name(),
                nsd.pool(),
                nsd.holds_metadata(),
                nsd.holds_objectdata(),
                nsd.size(),
            )?;
        }
    }

    writeln!(
        output,
        "# HELP gpfs_df_nsd_free_blocks GPFS mmdf NSD free blocks in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_nsd_free_blocks gauge")?;

    for (fs, nsds) in nsds {
        for nsd in nsds {
            writeln!(
                output,
                "gpfs_df_nsd_free_blocks{{name=\"{}\",fs=\"{fs}\",pool=\"{}\",metadata=\"{}\",data=\"{}\"}} {}",
                nsd.name(),
                nsd.pool(),
                nsd.holds_metadata(),
                nsd.holds_objectdata(),
                nsd.free_blocks(),
            )?;
        }
    }

    writeln!(
        output,
        "# HELP gpfs_df_nsd_free_blocks_percent GPFS mmdf NSD free blocks percent"
    )?;
    writeln!(output, "# TYPE gpfs_df_nsd_free_blocks_percent gauge")?;

    for (fs, nsds) in nsds {
        for nsd in nsds {
            writeln!(
                output,
                "gpfs_df_nsd_free_blocks_percent{{name=\"{}\",fs=\"{fs}\",pool=\"{}\",metadata=\"{}\",data=\"{}\"}} {}",
                nsd.name(),
                nsd.pool(),
                nsd.holds_metadata(),
                nsd.holds_objectdata(),
                nsd.free_blocks_percent(),
            )?;
        }
    }

    writeln!(
        output,
        "# HELP gpfs_df_nsd_free_fragments GPFS mmdf NSD free fragments in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_nsd_free_fragments gauge")?;

    for (fs, nsds) in nsds {
        for nsd in nsds {
            writeln!(
                output,
                "gpfs_df_nsd_free_fragments{{name=\"{}\",fs=\"{fs}\",pool=\"{}\",metadata=\"{}\",data=\"{}\"}} {}",
                nsd.name(),
                nsd.pool(),
                nsd.holds_metadata(),
                nsd.holds_objectdata(),
                nsd.free_fragments(),
            )?;
        }
    }

    writeln!(
        output,
        "# HELP gpfs_df_nsd_free_fragments_percent GPFS mmdf NSD free fragments percent"
    )?;
    writeln!(output, "# TYPE gpfs_df_nsd_free_fragments_percent gauge")?;

    for (fs, nsds) in nsds {
        for nsd in nsds {
            writeln!(
                output,
                "gpfs_df_nsd_free_fragments_percent{{name=\"{}\",fs=\"{fs}\",pool=\"{}\",metadata=\"{}\",data=\"{}\"}} {}",
                nsd.name(),
                nsd.pool(),
                nsd.holds_metadata(),
                nsd.holds_objectdata(),
                nsd.free_fragments_percent(),
            )?;
        }
    }

    Ok(())
}

/// Writes the `mmdf` pool data as prometheus metrics to `output`.
///
/// # Errors
///
/// This function uses [`writeln`] to write to `output`. It can only fail if
/// any of these [`writeln`] fails.
pub fn write_df_pool_metrics<H, O>(
    pools: &HashMap<String, Vec<crate::df::Pool>, H>,
    output: &mut O,
) -> Result<()>
where
    H: std::hash::BuildHasher,
    O: Write,
{
    writeln!(
        output,
        "# HELP gpfs_df_pool_size GPFS mmdf pool size in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_pool_size gauge")?;

    for (fs, pools) in pools {
        for pool in pools {
            writeln!(
                output,
                "gpfs_df_pool_size{{name=\"{}\",fs=\"{fs}\"}} {}",
                pool.name(),
                pool.size(),
            )?;
        }
    }

    writeln!(
        output,
        "# HELP gpfs_df_pool_free_blocks GPFS mmdf pool free blocks in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_pool_free_blocks gauge")?;

    for (fs, pools) in pools {
        for pool in pools {
            writeln!(
                output,
                "gpfs_df_pool_free_blocks{{name=\"{}\",fs=\"{fs}\"}} {}",
                pool.name(),
                pool.free_blocks(),
            )?;
        }
    }

    writeln!(
        output,
        "# HELP gpfs_df_pool_free_blocks_percent GPFS mmdf pool free blocks percent"
    )?;
    writeln!(output, "# TYPE gpfs_df_pool_free_blocks_percent gauge")?;

    for (fs, pools) in pools {
        for pool in pools {
            writeln!(
                output,
                "gpfs_df_pool_free_blocks_percent{{name=\"{}\",fs=\"{fs}\"}} {}",
                pool.name(),
                pool.free_blocks_percent(),
            )?;
        }
    }

    writeln!(
        output,
        "# HELP gpfs_df_pool_free_fragments GPFS mmdf pool free fragments in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_pool_free_fragments gauge")?;

    for (fs, pools) in pools {
        for pool in pools {
            writeln!(
                output,
                "gpfs_df_pool_free_fragments{{name=\"{}\",fs=\"{fs}\"}} {}",
                pool.name(),
                pool.free_fragments(),
            )?;
        }
    }

    writeln!(
        output,
        "# HELP gpfs_df_pool_free_fragments_percent GPFS mmdf pool free fragments percent"
    )?;
    writeln!(output, "# TYPE gpfs_df_pool_free_fragments_percent gauge")?;

    for (fs, pools) in pools {
        for pool in pools {
            writeln!(
                output,
                "gpfs_df_pool_free_fragments_percent{{name=\"{}\",fs=\"{fs}\"}} {}",
                pool.name(),
                pool.free_fragments_percent(),
            )?;
        }
    }

    Ok(())
}

/// Writes the `mmdf` file system total as prometheus metrics to `output`.
///
/// # Errors
///
/// This function uses [`writeln`] to write to `output`. It can only fail if
/// any of these [`writeln`] fails.
pub fn write_df_total_metrics<H, O>(
    filesystems: &HashMap<String, crate::df::Filesystem, H>,
    output: &mut O,
) -> Result<()>
where
    H: std::hash::BuildHasher,
    O: Write,
{
    writeln!(
        output,
        "# HELP gpfs_df_fs_size GPFS mmdf pool size in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_fs_size gauge")?;

    for (fs_name, fs) in filesystems {
        writeln!(
            output,
            "gpfs_df_fs_size{{name=\"{fs_name}\"}} {}",
            fs.size(),
        )?;
    }

    writeln!(
        output,
        "# HELP gpfs_df_fs_free_blocks GPFS mmdf pool free blocks in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_fs_free_blocks gauge")?;

    for (fs_name, fs) in filesystems {
        writeln!(
            output,
            "gpfs_df_fs_free_blocks{{name=\"{fs_name}\"}} {}",
            fs.free_blocks(),
        )?;
    }

    writeln!(
        output,
        "# HELP gpfs_df_fs_free_blocks_percent GPFS mmdf pool free blocks percent"
    )?;
    writeln!(output, "# TYPE gpfs_df_fs_free_blocks_percent gauge")?;

    for (fs_name, fs) in filesystems {
        writeln!(
            output,
            "gpfs_df_fs_free_blocks_percent{{name=\"{fs_name}\"}} {}",
            fs.free_blocks_percent(),
        )?;
    }

    writeln!(
        output,
        "# HELP gpfs_df_fs_free_fragments GPFS mmdf pool free fragments in kilobytes"
    )?;
    writeln!(output, "# TYPE gpfs_df_fs_free_fragments gauge")?;

    for (fs_name, fs) in filesystems {
        writeln!(
            output,
            "gpfs_df_fs_free_fragments{{name=\"{fs_name}\"}} {}",
            fs.free_fragments(),
        )?;
    }

    writeln!(
        output,
        "# HELP gpfs_df_fs_free_fragments_percent GPFS mmdf pool free fragments percent"
    )?;
    writeln!(output, "# TYPE gpfs_df_fs_free_fragments_percent gauge")?;

    for (fs_name, fs) in filesystems {
        writeln!(
            output,
            "gpfs_df_fs_free_fragments_percent{{name=\"{fs_name}\"}} {}",
            fs.free_fragments_percent(),
        )?;
    }

    Ok(())
}

/// Writes the filesets' information as prometheus metrics to `output`.
///
/// # Errors
///
/// This function uses [`writeln`] to write to `output`. It can only fail if
/// any of these [`writeln`] fails.
pub fn write_fileset_metrics<O>(
    filesets: &[Fileset],
    output: &mut O,
) -> Result<()>
where
    O: Write,
{
    writeln!(
        output,
        "# HELP gpfs_fileset_max_inodes GPFS fileset maximum inodes"
    )?;
    writeln!(output, "# TYPE gpfs_fileset_max_inodes gauge")?;

    for fileset in filesets {
        writeln!(
            output,
            "gpfs_fileset_max_inodes{{fs=\"{}\",fileset=\"{}\"}} {}",
            fileset.filesystem_name(),
            fileset.fileset_name(),
            fileset.max_inodes(),
        )?;
    }

    writeln!(
        output,
        "# HELP gpfs_fileset_alloc_inodes GPFS fileset allocated inodes"
    )?;
    writeln!(output, "# TYPE gpfs_fileset_alloc_inodes gauge")?;

    for fileset in filesets {
        writeln!(
            output,
            "gpfs_fileset_alloc_inodes{{fs=\"{}\",fileset=\"{}\"}} {}",
            fileset.filesystem_name(),
            fileset.fileset_name(),
            fileset.alloc_inodes(),
        )?;
    }

    Ok(())
}

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
