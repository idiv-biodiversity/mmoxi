//! Block device parsing.

use std::collections::HashMap;
use std::fs::DirEntry;
use std::str::FromStr;

use anyhow::{Context, Result};

/// Returns `sysfs` statistics for all block devices.
///
/// # Errors
///
/// Returns an error if parsing sysfs statistics fails.
pub fn stat_all() -> Result<HashMap<String, Stat>> {
    let mut ret = HashMap::default();

    let devices =
        std::fs::read_dir("/sys/block").context("read_dir call failed")?;

    for device in devices {
        let device = device.context("reading dir entry failed")?;
        let (device_name, stat) = stat(&device)?;
        ret.insert(device_name, stat);
    }

    Ok(ret)
}

fn stat(device: &DirEntry) -> Result<(String, Stat)> {
    let device_name = device.file_name().to_string_lossy().into();

    let stat_file = device.path().join("stat");

    let stat = std::fs::read_to_string(&stat_file)
        .with_context(|| {
            format!("reading stat file: {}", stat_file.display())
        })?
        .parse()?;

    Ok((device_name, stat))
}

/// Block device statistics. Time values are in milliseconds.
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default,
)]
pub struct Stat {
    /// Returns the number of read I/Os processed.
    pub read_ios: u64,

    /// Returns the number of read I/Os merged with in-queue I/O.
    pub read_merges: u64,

    /// Returns the number of sectors read.
    pub read_sectors: u64,

    /// Returns the total wait time for read requests.
    pub read_ticks: u64,

    /// Returns the number of write I/Os processed.
    pub write_ios: u64,

    /// Returns the number of write I/Os merged with in-queue I/O.
    pub write_merges: u64,

    /// Returns the number of sectors written.
    pub write_sectors: u64,

    /// Returns the total wait time for write requests.
    pub write_ticks: u64,

    /// Returns the number of I/Os currently in flight.
    pub in_flight: u64,

    /// Returns the total time this block device has been active.
    pub io_ticks: u64,

    /// Returns the total wait time for all requests.
    pub time_in_queue: u64,
}

impl Stat {
    /// Returns the total amount of bytes read (assuming 512 byte sectors).
    #[must_use]
    pub const fn read_bytes(&self) -> u64 {
        self.read_sectors * 512
    }

    /// Returns the total amount of bytes written (assuming 512 byte sectors).
    #[must_use]
    pub const fn write_bytes(&self) -> u64 {
        self.write_sectors * 512
    }
}

impl FromStr for Stat {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let mut stat = Self::default();

        let mut tokens =
            s.trim().split(' ').filter(|token| !token.trim().is_empty());

        stat.read_ios = tokens
            .next()
            .context("no read I/Os value")?
            .parse()
            .context("parsing read I/Os value")?;

        stat.read_merges = tokens
            .next()
            .context("no read merges value")?
            .parse()
            .context("parsing read merges value")?;

        stat.read_sectors = tokens
            .next()
            .context("no read sectors value")?
            .parse()
            .context("parsing read sectors value")?;

        stat.read_ticks = tokens
            .next()
            .context("no read ticks value")?
            .parse()
            .context("parsing read ticks value")?;

        stat.write_ios = tokens
            .next()
            .context("no write I/Os value")?
            .parse()
            .context("parsing write I/Os value")?;

        stat.write_merges = tokens
            .next()
            .context("no write merges value")?
            .parse()
            .context("parsing write merges value")?;

        stat.write_sectors = tokens
            .next()
            .context("no write sectors value")?
            .parse()
            .context("parsing write sectors value")?;

        stat.write_ticks = tokens
            .next()
            .context("no write ticks value")?
            .parse()
            .context("parsing write ticks value")?;

        stat.in_flight = tokens
            .next()
            .context("no in flight value")?
            .parse()
            .context("parsing in flight value")?;

        stat.io_ticks = tokens
            .next()
            .context("no I/O ticks value")?
            .parse()
            .context("parsing I/O ticks value")?;

        stat.time_in_queue = tokens
            .next()
            .context("no time in queue value")?
            .parse()
            .context("parsing time in queue value")?;

        Ok(stat)
    }
}

impl std::ops::Add for Stat {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            read_ios: self.read_ios + other.read_ios,
            read_merges: self.read_merges + other.read_merges,
            read_sectors: self.read_sectors + other.read_sectors,
            read_ticks: self.read_ticks + other.read_ticks,
            write_ios: self.write_ios + other.write_ios,
            write_merges: self.write_merges + other.write_merges,
            write_sectors: self.write_sectors + other.write_sectors,
            write_ticks: self.write_ticks + other.write_sectors,
            in_flight: self.in_flight + other.in_flight,
            io_ticks: self.io_ticks + other.io_ticks,
            time_in_queue: self.time_in_queue + other.time_in_queue,
        }
    }
}

impl std::ops::AddAssign for Stat {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            read_ios: self.read_ios + other.read_ios,
            read_merges: self.read_merges + other.read_merges,
            read_sectors: self.read_sectors + other.read_sectors,
            read_ticks: self.read_ticks + other.read_ticks,
            write_ios: self.write_ios + other.write_ios,
            write_merges: self.write_merges + other.write_merges,
            write_sectors: self.write_sectors + other.write_sectors,
            write_ticks: self.write_ticks + other.write_sectors,
            in_flight: self.in_flight + other.in_flight,
            io_ticks: self.io_ticks + other.io_ticks,
            time_in_queue: self.time_in_queue + other.time_in_queue,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_new() {
        let input = include_str!("block-example-new.in");

        let stat: Stat = input.parse().unwrap();

        assert_eq!(36290, stat.read_ios);
        assert_eq!(29495, stat.read_merges);
        assert_eq!(3_809_274, stat.read_sectors);
        assert_eq!(13845, stat.read_ticks);
        assert_eq!(128_563, stat.write_ios);
        assert_eq!(160_891, stat.write_merges);
        assert_eq!(8_436_842, stat.write_sectors);
        assert_eq!(99801, stat.write_ticks);
        assert_eq!(0, stat.in_flight);
        assert_eq!(125_014, stat.io_ticks);
        assert_eq!(154_343, stat.time_in_queue);
    }

    #[test]
    fn parse_old() {
        let input = include_str!("block-example-old.in");

        let stat: Stat = input.parse().unwrap();

        assert_eq!(79630, stat.read_ios);
        assert_eq!(116, stat.read_merges);
        assert_eq!(6_466_654, stat.read_sectors);
        assert_eq!(416_960, stat.read_ticks);
        assert_eq!(6_744_190, stat.write_ios);
        assert_eq!(8_022_528, stat.write_merges);
        assert_eq!(216_108_928, stat.write_sectors);
        assert_eq!(164_633_155, stat.write_ticks);
        assert_eq!(0, stat.in_flight);
        assert_eq!(33_293_921, stat.io_ticks);
        assert_eq!(165_047_391, stat.time_in_queue);
    }
}
