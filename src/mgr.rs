//! `mmlsmgr` parsing.

use std::io::BufRead;
use std::process::Command;

use anyhow::{anyhow, Context, Result};

/// Returns the cluster and filesystem managers.
///
/// # Errors
///
/// Returns an error if running `mmlsmgr` fails or if parsing its output fails.
pub fn get() -> Result<Manager> {
    let mut cmd = Command::new("mmlsmgr");
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {cmd:?}"))?;

    let manager = Manager::from_reader(output.stdout.as_slice())?;

    Ok(manager)
}

/// List of cluster and file system managers.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Manager {
    cluster: ClusterManager,
    fs: Vec<FSManager>,
}

impl Manager {
    /// Returns the cluster manager.
    #[must_use]
    pub const fn cluster(&self) -> &ClusterManager {
        &self.cluster
    }

    /// Returns the file system managers.
    #[must_use]
    pub const fn fs(&self) -> &Vec<FSManager> {
        &self.fs
    }
}

impl Manager {
    fn from_reader<Input: BufRead>(input: Input) -> Result<Self> {
        let mut cluster_index = ClusterIndex::default();
        let mut fs_index = FSIndex::default();
        let mut manager = Self::default();

        for line in input.lines() {
            let line = line?;

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[1] == "clusterManager" {
                if tokens[2] == "HEADER" {
                    cluster_index = ClusterIndex::default();
                    cluster_index.with_tokens(&tokens);
                } else {
                    manager.cluster =
                        ClusterManager::from_tokens(&tokens, &cluster_index)?;
                }
            } else if tokens[1] == "filesystemManager" {
                if tokens[2] == "HEADER" {
                    fs_index = FSIndex::default();
                    fs_index.with_tokens(&tokens);
                } else {
                    let fs = FSManager::from_tokens(&tokens, &fs_index)?;
                    manager.fs.push(fs);
                }
            }
        }

        Ok(manager)
    }
}

/// Cluster manager.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct ClusterManager {
    name: String,
}

impl ClusterManager {
    /// Returns the name of the cluster manager.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Filesystem manager.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct FSManager {
    fs: String,
    name: String,
    ip: String,
}

impl FSManager {
    /// Returns the filesystem name.
    #[must_use]
    pub fn fs(&self) -> &str {
        &self.fs
    }

    /// Returns the name of the manager.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the IP address of the manager.
    #[must_use]
    pub fn ip(&self) -> &str {
        &self.ip
    }
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl ClusterManager {
    fn from_tokens(tokens: &[&str], index: &ClusterIndex) -> Result<Self> {
        let manager_index =
            index.manager.ok_or_else(|| anyhow!("no manager index"))?;
        let manager = tokens[manager_index].into();

        Ok(Self { name: manager })
    }
}

impl FSManager {
    fn from_tokens(tokens: &[&str], index: &FSIndex) -> Result<Self> {
        let manager_index =
            index.manager.ok_or_else(|| anyhow!("no manager index"))?;
        let name = tokens[manager_index].into();

        let fs_index =
            index.fs.ok_or_else(|| anyhow!("no filesystem index"))?;
        let fs = tokens[fs_index].into();

        let ip_index =
            index.ip.ok_or_else(|| anyhow!("no managerIP index"))?;
        let ip = tokens[ip_index].into();

        Ok(Self { fs, name, ip })
    }
}

#[derive(Debug, Default)]
struct ClusterIndex {
    manager: Option<usize>,
}

impl ClusterIndex {
    fn with_tokens(&mut self, tokens: &[&str]) {
        for (i, token) in tokens.iter().enumerate() {
            if *token == "manager" {
                self.manager = Some(i);
            }
        }
    }
}

#[derive(Debug, Default)]
struct FSIndex {
    fs: Option<usize>,
    manager: Option<usize>,
    ip: Option<usize>,
}

impl FSIndex {
    fn with_tokens(&mut self, tokens: &[&str]) {
        for (i, token) in tokens.iter().enumerate() {
            match *token {
                "filesystem" => self.fs = Some(i),
                "manager" => self.manager = Some(i),
                "managerIP" => self.ip = Some(i),
                _ => {}
            }
        }
    }
}

// ----------------------------------------------------------------------------
// tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager() {
        let input = include_str!("mgr-example.in");

        let manager = Manager::from_reader(input.as_bytes()).unwrap();

        assert_eq!(
            manager,
            Manager {
                cluster: ClusterManager {
                    name: "filer1".into()
                },
                fs: vec![
                    FSManager {
                        fs: "gpfs1".into(),
                        name: "filer2".into(),
                        ip: "10.10.21.2".into(),
                    },
                    FSManager {
                        fs: "gpfs2".into(),
                        name: "filer3".into(),
                        ip: "10.10.21.3".into(),
                    },
                ]
            }
        );
    }
}
