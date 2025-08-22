#![deny(clippy::all)]
#![warn(clippy::pedantic, clippy::nursery, clippy::cargo)]

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;

use mmoxi::prom::ToText;

mod cli;

fn main() -> Result<()> {
    let args = cli::args();

    match args.subcommand() {
        Some(("cache", args)) => dispatch_cache(args),
        Some(("list", args)) => dispatch_list(args),
        Some(("pool-percent", args)) => run_pool_percent(args),
        Some(("prometheus", args)) => dispatch_prom(args),
        Some(("show", args)) => dispatch_show(args),

        _ => Err(anyhow!("subcommand is required")),
    }
}

// ----------------------------------------------------------------------------
// subcommand dispatcher
// ----------------------------------------------------------------------------

fn dispatch_cache(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("nmon", args)) => run_cache_nmon(args),
        Some(("nsds", args)) => run_cache_nsds(args),

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_list(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("filesystems", _args)) => run_list_filesystems(),

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_prom(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("df", args)) => run_prom_df(args),
        Some(("disk", args)) => run_prom_disk(args),
        Some(("fileset", args)) => run_prom_fileset(args),
        Some(("manager", args)) => run_prom_manager(args),
        Some(("pool", args)) => dispatch_prom_pool(args),
        Some(("quota", args)) => run_prom_quota(args),

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_prom_pool(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("block", args)) => run_prom_pool_block(args),
        Some(("usage", args)) => run_prom_pool_usage(args),
        Some(("user-distribution", args)) => {
            run_prom_pool_user_distribution(args)
        }

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_show(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("manager", args)) => dispatch_show_manager(args),
        Some(("node", _args)) => run_show_node(),

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_show_manager(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("cluster", _args)) => run_show_cluster_manager(),
        Some(("filesystem", args)) => run_show_filesystem_manager(args),

        _ => Err(anyhow!("subcommand is required")),
    }
}

// ----------------------------------------------------------------------------
// runner
// ----------------------------------------------------------------------------

fn run_cache_nmon(args: &ArgMatches) -> Result<()> {
    let force = args.contains_id("force");

    let device_cache = args
        .get_one::<PathBuf>("device-cache")
        .expect("device-cache has a default value");

    let output = args
        .get_one::<PathBuf>("output")
        .expect("output has a default value");

    let output = File::create(output).with_context(|| {
        format!("creating output file: {}", output.display())
    })?;

    let mut output = BufWriter::new(output);

    mmoxi::nmon::by_pool_cached(device_cache, force, &mut output)
}

fn run_cache_nsds(args: &ArgMatches) -> Result<()> {
    let force = args.contains_id("force");

    let output = args
        .get_one::<PathBuf>("output")
        .expect("output has a default value");

    let _nsds = mmoxi::nsd::local_cached(output, force)?;

    Ok(())
}

fn run_list_filesystems() -> Result<()> {
    let names = mmoxi::fs::names()?;

    for name in names {
        println!("{name}");
    }

    Ok(())
}

fn run_pool_percent(args: &ArgMatches) -> Result<()> {
    let filesystem = args
        .get_one::<String>("filesystem")
        .expect("filesystem is a required argument");

    let filesystem = mmoxi::pool::run(filesystem)?;

    let pool_arg = args
        .get_one::<String>("pool")
        .expect("pool is a required argument");

    let pool = filesystem
        .pools()
        .iter()
        .find(|pool| pool.name() == pool_arg)
        .with_context(|| format!("pool {pool_arg} not found"))?;

    let data_pool_size = pool
        .data()
        .with_context(|| format!("pool {pool_arg} is not object data"))?;

    println!("{}", data_pool_size.used_percent());

    Ok(())
}

fn run_prom_pool_user_distribution(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let device_or_dir = args
        .get_one::<String>("device-or-dir")
        .expect("device-or-dir is a required argument");

    let pool = args
        .get_one::<String>("pool")
        .expect("pool is a required argument");

    let fileset = args.get_one::<String>("fileset");

    let nodes = args.get_one::<String>("nodes");

    let local_work_dir = args.get_one::<PathBuf>("local-work-dir");
    let global_work_dir = args.get_one::<PathBuf>("global-work-dir");

    let scope = args.get_one::<String>("scope");

    let data = mmoxi::policy::pool_user_distribution::run(
        device_or_dir,
        pool,
        fileset,
        nodes,
        local_work_dir,
        global_work_dir,
        scope,
    )?;

    data.to_prom(&mut output)?;

    Ok(())
}

fn run_prom_df(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;
    let data = mmoxi::df::run()?;
    data.to_prom(&mut output)?;
    Ok(())
}

fn run_prom_disk(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let mut all_disks = HashMap::new();

    for fs in mmoxi::fs::names()? {
        let disks = mmoxi::disk::disks(&fs)?;
        all_disks.insert(fs, disks);
    }

    all_disks
        .to_prom(&mut output)
        .context("converting internal data to prometheus")?;

    Ok(())
}

fn run_prom_fileset(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let mut filesets = vec![];

    for fs in mmoxi::fs::names()? {
        filesets.extend(mmoxi::fileset::filesets(&fs)?);
    }

    filesets.to_prom(&mut output)?;

    Ok(())
}

fn run_prom_manager(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let data = mmoxi::mgr::get()?;
    data.to_prom(&mut output)?;

    Ok(())
}

fn run_prom_pool_block(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let device_cache = args
        .get_one::<PathBuf>("device-cache")
        .expect("device-cache has a default value");

    let force = args.contains_id("force");

    let metrics = mmoxi::prom::pool_block_device_metrics(device_cache, force)?;
    metrics.to_prom(&mut output)?;

    Ok(())
}

fn run_prom_pool_usage(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let names = mmoxi::fs::names()?;

    let filesystems = mmoxi::pool::run_all(&names)?;

    filesystems
        .to_prom(&mut output)
        .context("converting internal data to prometheus")?;

    Ok(())
}

fn run_prom_quota(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let data = mmoxi::quota::Data::from_reader(io::stdin().lock())?;
    data.to_prom(&mut output)?;

    Ok(())
}

fn run_show_cluster_manager() -> Result<()> {
    let managers = mmoxi::mgr::get()?;

    println!("{}", managers.cluster().name());

    Ok(())
}

fn run_show_filesystem_manager(args: &ArgMatches) -> Result<()> {
    let filesystem_name = args
        .get_one::<String>("filesystem")
        .expect("filesystem is a required argument");

    let managers = mmoxi::mgr::get()?;

    let Some(manager) = managers
        .fs()
        .iter()
        .find(|manager| manager.fs_name() == filesystem_name)
    else {
        return Err(anyhow!("filesystem not found in manager list"));
    };

    println!("{}", manager.manager_name());

    Ok(())
}

fn run_show_node() -> Result<()> {
    let node = mmoxi::state::local_node_name()
        .context("determining local node name")?;

    println!("{node}");

    Ok(())
}

// ----------------------------------------------------------------------------
// helper
// ----------------------------------------------------------------------------

fn output_to_bufwriter(
    args: &ArgMatches,
) -> Result<BufWriter<Box<dyn Write>>> {
    let output = args.get_one::<PathBuf>("output");

    let output: Box<dyn Write> = if let Some(ref output) = output {
        Box::new(File::create(output)?)
    } else {
        Box::new(io::stdout())
    };

    Ok(BufWriter::new(output))
}
