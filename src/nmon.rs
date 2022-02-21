//! `nmon` integration.

use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};

use crate::nsd::{ByPool, Nsd};

/// Returns the default local device cache path.
pub const DEFAULT_DEVICE_CACHE: &str = "/run/mmlocal-nmon-cache";

/// Writes `nmon` disk groups to `output`.
///
/// If the `cache` file exists, try reading from that. If it doesn't exist,
/// fetch the local NSDs, fill the cache and return the result. Recreating the
/// cache can be en`force`d.
///
/// The groups are `{fs}-{pool}`-tuples, each containing all local disks for
/// that respective pool.
///
/// # Errors
///
/// Returns an error if reading from the cache fails, if either of the used
/// `mm*` commands fails, or if writing to the output stream fails.
pub fn by_pool_cached<Cache, Output>(
    device_cache: Cache,
    force: bool,
    output: &mut Output,
) -> Result<()>
where
    Cache: AsRef<Path>,
    Output: Write,
{
    let device_cache = device_cache.as_ref();

    let pooled = crate::nsd::local_pooled(device_cache, force)
        .context("fetching local NSD devices")?;

    by_pool(pooled, output)?;

    Ok(())
}

/// Writes `nmon` disk groups to `output`.
///
/// The groups are `{fs}-{pool}`-tuples, each containing all local disks for
/// that respective pool.
///
/// # Errors
///
/// Returns an error if writing to the output stream fails.
pub fn by_pool<Output>(pooled: ByPool, output: &mut Output) -> Result<()>
where
    Output: Write,
{
    for (id, nsds) in pooled {
        let fs = id.fs();
        let pool = id.pool();

        let devices = nsds
            .iter()
            .flat_map(Nsd::device_name)
            .collect::<Vec<_>>()
            .join(" ");

        writeln!(output, "{fs}-{pool} {devices}")?;
    }

    Ok(())
}
