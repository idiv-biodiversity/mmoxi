use std::path::PathBuf;

use clap::{crate_name, crate_version};
use clap::{Arg, ArgAction, ArgMatches, Command};

pub fn args() -> ArgMatches {
    build().get_matches()
}

pub fn build() -> Command {
    let fs = Arg::new("filesystem").required(true).help("file system");

    let pool = Arg::new("pool").required(true).help("pool name");

    let pool_percent = Command::new("pool-percent")
        .about("show pool used in percent")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(fs)
        .arg(pool);

    Command::new(crate_name!())
        .version(crate_version!())
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(build_cache())
        .subcommand(build_list())
        .subcommand(pool_percent)
        .subcommand(build_prometheus())
}

fn build_cache() -> Command {
    let cache_nmon = Command::new("nmon")
        .about("cache local NSD block devices for use with nmon")
        .arg(arg_device_cache())
        .arg(arg_force())
        .arg(
            arg_output()
                .default_value(mmoxi::nmon::DEFAULT_DEVICE_CACHE),
        )
        .after_long_help(
"The local NSD block device association needs to be figured out with \
 `mmlsnsd -X`, which is an expensive operation. That's why this caching \
 command exists."
        );

    let cache_nsds = Command::new("nsds")
        .about("cache local NSD block device association")
        .arg(arg_force())
        .arg(
            arg_output()
                .default_value(mmoxi::nsd::DEFAULT_LOCAL_DEVICE_CACHE),
        )
        .after_long_help(
"The local NSD block device association needs to be figured out with \
 `mmlsnsd -X`, which is an expensive operation. That's why this caching \
 command exists."
        );

    Command::new("cache")
        .about("cache results for later use")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(cache_nmon)
        .subcommand(cache_nsds)
}

fn build_list() -> Command {
    let list_fs = Command::new("filesystems")
        .about("list file system names")
        .alias("fs")
        .disable_help_flag(true)
        .disable_version_flag(true);

    Command::new("list")
        .about("list commands")
        .alias("ls")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(list_fs)
}

pub fn build_prometheus() -> Command {
    let prom_pool_block = Command::new("block")
        .about("Gather block device metrics grouped by pool.")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(arg_device_cache())
        .arg(arg_force())
        .arg(arg_output())
        .after_long_help("Run locally on every file server.");

    let prom_pool_usage = Command::new("usage")
        .about("Gather pool usage metrics.")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(arg_output())
        .after_long_help("Run on cluster manager only.");

    let prom_pool = Command::new("pool")
        .about("Pool metrics.")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(prom_pool_block)
        .subcommand(prom_pool_usage);

    let prom_quota = Command::new("quota")
        .about("Quota metrics.")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(arg_output())
        .after_long_help("Run every hour on cluster manager only.");

    Command::new("prometheus")
        .about("prometheus metrics")
        .alias("prom")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(prom_pool)
        .subcommand(prom_quota)
}

fn arg_device_cache() -> Arg {
    Arg::new("device-cache")
        .long("device-cache")
        .value_parser(clap::value_parser!(PathBuf))
        .default_value(mmoxi::nsd::DEFAULT_LOCAL_DEVICE_CACHE)
        .help("local NSD block device cache")
        .long_help("Cache for local NSD block device associations.")
}

fn arg_force() -> Arg {
    Arg::new("force")
        .short('f')
        .long("force")
        .action(ArgAction::SetTrue)
        .help("force cache recreation")
        .long_help("Force recreating the cache.")
}

fn arg_output() -> Arg {
    Arg::new("output")
        .short('o')
        .long("output")
        .value_parser(clap::value_parser!(PathBuf))
        .help("output file")
        .long_help("Output file.")
}
