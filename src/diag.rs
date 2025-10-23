//! `mmdiag` parsing.

use std::io::{BufRead, Write};
use std::process::Command;

use anyhow::{Context, Result, anyhow};

/// Returns the deadlock.
///
/// # Errors
///
/// Returns an error if running `mmlsmgr` fails or if parsing its output fails.
pub fn deadlock() -> Result<Deadlock> {
    let mut cmd = Command::new("mmdiag");
    cmd.arg("--deadlock");
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {cmd:?}"))?;

    let data = Deadlock::from_reader(output.stdout.as_slice())?;

    Ok(data)
}

/// Deadlock.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Deadlock {
    node_list: Vec<String>,
}

impl Deadlock {
    fn from_reader<Input: BufRead>(input: Input) -> Result<Self> {
        let mut index = Index::default();
        let mut data = Self::default();

        for line in input.lines() {
            let line = line?;

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[1] == "deadlockNodes" {
                if tokens[2] == "HEADER" {
                    index = Index::default();
                    index.with_tokens(&tokens);
                } else {
                    let new = Self::from_tokens(&tokens, &index)?;
                    data.node_list.extend(new.node_list);
                }
            }
        }

        Ok(data)
    }
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl Deadlock {
    fn from_tokens(tokens: &[&str], index: &Index) -> Result<Self> {
        let node_list = index
            .node_list
            .ok_or_else(|| anyhow!("no node name index"))?;
        let node_list = tokens[node_list].into();

        Ok(Self {
            node_list: vec![node_list],
        })
    }
}

#[derive(Debug, Default)]
struct Index {
    node_list: Option<usize>,
}

impl Index {
    fn with_tokens(&mut self, tokens: &[&str]) {
        for (i, token) in tokens.iter().enumerate() {
            if *token == "nodeList" {
                self.node_list = Some(i);
            }
        }
    }
}

// ----------------------------------------------------------------------------
// prometheus
// ----------------------------------------------------------------------------

impl crate::prom::ToText for Deadlock {
    fn to_prom(&self, output: &mut impl Write) -> Result<()> {
        writeln!(output, "# HELP gpfs_diag_deadlocks GPFS deadlock nodes.")?;
        writeln!(output, "# TYPE gpfs_diag_deadlocks gauge")?;
        writeln!(output, "gpfs_diag_deadlocks {}", self.node_list.len())?;

        Ok(())
    }
}

// ----------------------------------------------------------------------------
// tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty() {
        let input = include_str!("diag-deadlock-example-empty.in");

        let data = Deadlock::from_reader(input.as_bytes()).unwrap();

        assert_eq!(data, Deadlock { node_list: vec![] },);
    }

    #[test]
    fn parse() {
        let input = include_str!("diag-deadlock-example.in");

        let data = Deadlock::from_reader(input.as_bytes()).unwrap();

        assert_eq!(
            data,
            Deadlock {
                node_list: vec!["filer3-ib0".into()]
            },
        );
    }
}
