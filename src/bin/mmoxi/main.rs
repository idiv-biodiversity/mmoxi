#![deny(clippy::all)]
#![warn(clippy::pedantic, clippy::nursery, clippy::cargo)]

use std::fs::File;
use std::io::{self, BufWriter, Write};

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

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_prom(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("pool", args)) => dispatch_prom_pool(args),
        Some(("quota", args)) => run_prom_quota(args),

        _ => Err(anyhow!("subcommand is required")),
    }
}

fn dispatch_prom_pool(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("block", args)) => run_prom_pool_block(args),
        Some(("usage", args)) => run_prom_pool_usage(args),

        _ => Err(anyhow!("subcommand is required")),
    }
}

// ----------------------------------------------------------------------------
// runner
// ----------------------------------------------------------------------------

fn run_cache_nmon(args: &ArgMatches) -> Result<()> {
    let force = args.is_present("force");

    // UNWRAP has default
    let device_cache = args.value_of("device-cache").unwrap();

    // UNWRAP has default
    let output = args.value_of("output").unwrap();

    let output = File::create(output)
        .with_context(|| format!("creating output file: {output}"))?;

    let mut output = BufWriter::new(output);

    mmoxi::nmon::by_pool_cached(device_cache, force, &mut output)
}

fn run_cache_nsds(args: &ArgMatches) -> Result<()> {
    let force = args.is_present("force");

    // UNWRAP has default
    let output = args.value_of("output").unwrap();

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
        .value_of("filesystem")
        .context("no filesystem argument")?;

    let filesystem = mmoxi::pool::run(filesystem)?;

    let pool_arg = args.value_of("pool").context("no pool argument")?;

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

fn run_prom_pool_block(args: &ArgMatches) -> Result<()> {
    let mut output = output_to_bufwriter(args)?;

    // UNWRAP has default
    let device_cache = args.value_of("device-cache").unwrap();

    let force = args.is_present("force");

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
    let output = args.value_of("output");

    let output: Box<dyn Write> = if let Some(output) = output {
        Box::new(File::create(output)?)
    } else {
        Box::new(io::stdout())
    };

    Ok(BufWriter::new(output))
}
