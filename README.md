mmoxi
=====

A [Rust][] library and collection of tools for [IBM Spectrum Scale][] (formerly
GPFS) file systems.


Motivation
----------

The [Rust][] library at the moment is mostly `mm* -Y` parsing for easier access
in tooling. We did all the `mm* -Y` parsing and tooling in shell scripts
before. You can certainly do this, but writing good tooling in bash just
doesn't scale. Having a library for access to the parsed output just makes
writing tools so much easier.

Notable exceptions to parsing in the library and listing in tooling are:

1.  An [nmon][] integration that groups NSDs by `(fs,pool)`-tuple to be shown
    in `nmon`s **Disk Group I/O** mode:

    ```bash
    mmoxi cache nmon
    NMON=g nmon -s 1 -d 1024 -g '/run/mmlocal-nmon-cache'
    ```

    This gives a better view of file servers and how much I/O they do with NSD
    groups rather than individual NSDs or everything summed up.

1.  Prometheus metrics for:

    - disk pool size
    - disk pool group I/O
    - quotas


Docs and Usage
--------------

See the [docs][] for library documentation and `mmoxi help [subcommand...]` for
the CLI tools.


Installation
------------

### cargo install

```bash
cargo install mmoxi
```

### from source

```bash
git clone https://github.com/idiv-biodiversity/mmoxi.git
cd mmoxi
cargo build --release
install -Dm755 target/release/mmoxi ~/bin/mmoxi
```


[IBM Spectrum Scale]: https://www.ibm.com/products/spectrum-scale
[Rust]: https://www.rust-lang.org/
[nmon]: https://nmon.sourceforge.net/
[docs]: https://docs.rs/mmoxi
