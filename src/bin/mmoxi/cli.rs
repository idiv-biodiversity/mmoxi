use std::fs;
use std::path::PathBuf;

use clap::builder::PossibleValuesParser;
use clap::{crate_name, crate_version};
use clap::{Arg, ArgAction, ArgMatches, Command};

pub fn args() -> ArgMatches {
    build().get_matches()
}

pub fn build() -> Command {
    let pool_percent = Command::new("pool-percent")
        .about("show pool used in percent")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(arg_filesystem())
        .arg(arg_pool());

    Command::new(crate_name!())
        .version(crate_version!())
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .infer_subcommands(true)
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

    let list_cluster_mgr = Command::new("cluster")
        .about("list cluster manager")
        .disable_help_flag(true)
        .disable_version_flag(true);

    let list_mgr = Command::new("manager")
        .about("list cluster and file system managers")
        .alias("mgr")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(list_cluster_mgr);

    Command::new("list")
        .about("list commands")
        .alias("ls")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(list_fs)
        .subcommand(list_mgr)
}

pub fn build_prometheus() -> Command {
    let prom_df = Command::new("df")
        .about("Gather metrics from mmdf.")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(arg_output())
        .after_long_help("Run on cluster manager only.");

    let prom_disk = Command::new("disk")
        .about("Gather metrics from mmlsdisk.")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(arg_output())
        .after_long_help("Run on cluster manager only.");

    let prom_fileset = Command::new("fileset")
        .about("Gather fileset metrics.")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(arg_output())
        .after_long_help("Run on cluster manager only.");

    let prom_pool_user_distribution = Command::new("user-distribution")
        .about("Gather usage per user for a pool.")
        .alias("udistri")
        .arg(arg_output())
        .args(policy_args())
        .disable_help_flag(true)
        .disable_version_flag(true)
        .after_long_help(
"This is useful to figure out which users are heavily using expensive storage \
 pools like NVME storage. Run on cluster manager only."
        );

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
        .subcommand(prom_pool_usage)
        .subcommand(prom_pool_user_distribution);

    let prom_quota = Command::new("quota")
        .about("Gather quota metrics.")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(arg_output())
        .after_long_help(
"Run every hour on cluster manager only. This command expects piped output \
 from one or more `mmrepquota` commands, e.g. `{ mmrepquota -Y -j gpfs1; \
 mmrepquota -Y -u gpfs1:work; } | mmoxi prom quota");

    Command::new("prometheus")
        .about("prometheus metrics")
        .alias("prom")
        .disable_help_flag(true)
        .disable_version_flag(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(prom_df)
        .subcommand(prom_disk)
        .subcommand(prom_fileset)
        .subcommand(prom_pool)
        .subcommand(prom_quota)
}

// ----------------------------------------------------------------------------
// arguments
// ----------------------------------------------------------------------------

fn arg_device_cache() -> Arg {
    Arg::new("device-cache")
        .long("device-cache")
        .value_parser(clap::value_parser!(PathBuf))
        .default_value(mmoxi::nsd::DEFAULT_LOCAL_DEVICE_CACHE)
        .help("local NSD block device cache")
        .long_help("Cache for local NSD block device associations.")
}

fn arg_filesystem() -> Arg {
    Arg::new("filesystem")
        .required(true)
        .action(ArgAction::Set)
        .help("file system")
        .long_help("File system name.")
        .value_name("filesystem")
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

fn arg_pool() -> Arg {
    Arg::new("pool")
        .required(true)
        .action(ArgAction::Set)
        .help("pool name")
        .long_help("Specify pool name.")
        .value_name("pool")
}

fn policy_args() -> Vec<Arg> {
    vec![
        Arg::new("device-or-dir")
            .required(true)
            .action(ArgAction::Set)
            .help("device or directory")
            .long_help("Specify device or directory to use with `mmapplypolicy`.")
            .value_name("Device|Directory"),

        arg_pool(),

        Arg::new("fileset")
            .long("fileset")
            .action(ArgAction::Set)
            .help("filter by fileset")
            .long_help("Filter by fileset.")
            .value_name("fileset"),

        Arg::new("nodes")
            .short('N')
            .long("nodes")
            .action(ArgAction::Set)
            .help("use for mmapplypolicy -N argument")
            .long_help(
"Specify list of nodes to use with `mmapplypolicy -N`. For detailed \
 information, see `man mmapplypolicy`.",
            )
            .value_name("all|mount|Node,...|NodeFile|NodeClass"),

        Arg::new("global-work-dir")
            .short('g')
            .long("global-work-dir")
            .help("use for mmapplypolicy -g argument")
            .long_help(
"Specify global work directory to use with `mmapplypolicy -g`. For detailed \
 information, see `man mmapplypolicy`.",
            )
            .action(ArgAction::Set)
            .value_name("dir")
            .value_parser(is_dir),

        Arg::new("local-work-dir")
            .short('s')
            .long("local-work-dir")
            .help("use for mmapplypolicy -s argument and policy output")
            .long_help(
"Specify local work directory to use with `mmapplypolicy -s`. Also, the \
 output of the LIST policies will be written to this directory temporarily \
 before being processed by this tool. Defaults to the system temporary \
 directory. This might be too small for large directories, e.g. more than 30 \
 GiB are needed for a directory with 180 million files. For detailed \
 information about the `-s` argument, see `man mmapplypolicy`.",
            )
            .action(ArgAction::Set)
            .value_name("dir")
            .value_parser(is_dir),

        Arg::new("scope")
            .long("scope")
            .help("specify scope of the policy scan")
            .long_help(
"Specify the scope of the policy scan. For detailed information, see `man \
 mmapplypolicy`."
            )
            .action(ArgAction::Set)
            .value_name("filesystem|inodespace|fileset")
            .value_parser(PossibleValuesParser::new(
                ["filesystem", "inodespace", "fileset"]
            ))
    ]
}

// ----------------------------------------------------------------------------
// value parser
// ----------------------------------------------------------------------------

fn is_dir(s: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(s);

    if !path.exists() {
        Err(format!("does not exist: {}", path.display()))
    } else if !path.is_dir() {
        Err(format!("is not a directory: {}", path.display()))
    } else if let Err(error) = fs::read_dir(&path) {
        Err(error.to_string())
    } else {
        Ok(path)
    }
}
