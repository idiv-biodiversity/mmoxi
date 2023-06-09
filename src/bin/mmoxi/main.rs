#![deny(clippy::all)]
#![warn(clippy::pedantic, clippy::nursery, clippy::cargo)]

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::ArgMatches;

mod cli;

fn main() -> Result<()> {
    let args = cli::args();

    match args.subcommand() {
        Some(("cache", args)) => dispatch_cache(args),
        Some(("list", args)) => dispatch_list(args),
        Some(("pool-percent", args)) => run_pool_percent(args),
        Some(("prometheus", args)) => dispatch_prom(args),

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
        Some(("manager", args)) => dispatch_list_manager(args),

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_list_manager(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("cluster", _args)) => run_list_mgr_cluster(),

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_prom(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("df", args)) => run_prom_df(args),
        Some(("fileset", args)) => run_prom_fileset(args),
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

// ----------------------------------------------------------------------------
// runner
// ----------------------------------------------------------------------------

fn run_cache_nmon(args: &ArgMatches) -> Result<()> {
    let force = args.contains_id("force");

    // UNWRAP has default
    let device_cache = args.get_one::<PathBuf>("device-cache").unwrap();

    // UNWRAP has default
    let output = args.get_one::<PathBuf>("output").unwrap();

    let output = File::create(output).with_context(|| {
        format!("creating output file: {}", output.display())
    })?;

    let mut output = BufWriter::new(output);

    mmoxi::nmon::by_pool_cached(device_cache, force, &mut output)
}

fn run_cache_nsds(args: &ArgMatches) -> Result<()> {
    let force = args.contains_id("force");

    // UNWRAP has default
    let output = args.get_one::<PathBuf>("output").unwrap();

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

fn run_list_mgr_cluster() -> Result<()> {
    let managers = mmoxi::mgr::get()?;

    println!("{}", managers.cluster().name());

    Ok(())
}

fn run_pool_percent(args: &ArgMatches) -> Result<()> {
    let filesystem = args
        .get_one::<String>("filesystem")
        .context("no filesystem argument")?;

    let filesystem = mmoxi::pool::run(filesystem)?;

    let pool_arg =
        args.get_one::<String>("pool").context("no pool argument")?;

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
        .context("device or directory is required")?;

    let pool = args.get_one::<String>("pool").context("pool is required")?;

    let fileset = args.get_one::<String>("fileset");

    let nodes = args.get_one::<String>("nodes");

    let local_work_dir = args.get_one::<PathBuf>("local-work-dir");
    let global_work_dir = args.get_one::<PathBuf>("global-work-dir");

    let scope = args.get_one::<String>("scope");

    let mut user_sizes = mmoxi::policy::pool_user_distribution::run(
        device_or_dir,
        pool,
        fileset,
        nodes,
        local_work_dir,
        global_work_dir,
        scope,
    )?;

    let mut named_user_sizes = HashMap::with_capacity(user_sizes.len());

    for (user, data) in user_sizes.drain() {
        let user = mmoxi::user::by_uid(&user).unwrap_or(user);
        named_user_sizes.insert(user, data);
    }

    mmoxi::prom::write_pool_user_distribution(
        pool,
        &named_user_sizes,
        &mut output,
    )?;

    Ok(())
}

fn run_prom_df(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let mut all_nsds: HashMap<String, Vec<mmoxi::df::Nsd>> =
        HashMap::default();

    let mut all_pools: HashMap<String, Vec<mmoxi::df::Pool>> =
        HashMap::default();

    let mut all_totals: HashMap<String, mmoxi::df::Filesystem> =
        HashMap::default();

    for fs in mmoxi::fs::names()? {
        let mmoxi::df::Data {
            fs,
            nsds,
            pools,
            total,
        } = mmoxi::df::run(&fs)?;

        all_nsds.insert(fs.clone(), nsds);
        all_pools.insert(fs.clone(), pools);
        all_totals.insert(fs, total);
    }

    mmoxi::prom::write_df_nsd_metrics(&all_nsds, &mut output)?;
    mmoxi::prom::write_df_pool_metrics(&all_pools, &mut output)?;
    mmoxi::prom::write_df_total_metrics(&all_totals, &mut output)?;

    Ok(())
}

fn run_prom_fileset(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let mut filesets = vec![];

    for fs in mmoxi::fs::names()? {
        filesets.extend(mmoxi::fileset::filesets(&fs)?);
    }

    mmoxi::prom::write_fileset_metrics(&filesets, &mut output)?;

    Ok(())
}

fn run_prom_pool_block(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    // UNWRAP has default
    let device_cache = args.get_one::<PathBuf>("device-cache").unwrap();

    let force = args.contains_id("force");

    let metrics = mmoxi::prom::pool_block_device_metrics(device_cache, force)?;
    metrics.to_prom(&mut output)?;

    Ok(())
}

fn run_prom_pool_usage(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let names = mmoxi::fs::names()?;

    let filesystems = mmoxi::pool::run_all(&names)?;

    mmoxi::pool::to_prom(&filesystems, &mut output)
        .context("converting internal data to prometheus")?;

    Ok(())
}

fn run_prom_quota(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    let data = mmoxi::quota::Data::from_reader(io::stdin().lock())?;
    data.to_prom(&mut output)?;

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
