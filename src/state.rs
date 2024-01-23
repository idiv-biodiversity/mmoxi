//! `mmgetstate` parsing.

use std::io::BufRead;
use std::process::Command;

use anyhow::{anyhow, Context, Result};

/// Returns the local node name.
///
/// # Errors
///
/// Returns an error if running `mmgetstate` fails or if parsing its output fails.
pub fn local_node_name() -> Result<String> {
    let mut cmd = Command::new("mmgetstate");
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {cmd:?}"))?;

    let states = States::from_reader(output.stdout.as_slice())?;

    states
        .0
        .into_iter()
        .next()
        .map(State::into_name)
        .ok_or_else(|| anyhow!("no local state"))
}

/// Parsed states.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct States(Vec<State>);

impl States {
    fn from_reader<Input: BufRead>(input: Input) -> Result<Self> {
        let mut index = Index::default();
        let mut nsds = Self::default();

        for line in input.lines() {
            let line = line?;

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[2] == "HEADER" {
                index = Index::default();
                header_to_index(&tokens, &mut index);
            } else {
                let entry = State::from_tokens(&tokens, &index)?;
                nsds.0.push(entry);
            }
        }

        Ok(nsds)
    }

    /// Returns the states.
    #[must_use]
    pub fn states(&self) -> &[State] {
        &self.0
    }
}

/// State data.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct State {
    name: String,
    state: String,
}

impl State {
    /// Returns the node name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the node name.
    #[must_use]
    pub fn into_name(self) -> String {
        self.name
    }

    /// Returns the state.
    #[must_use]
    pub fn state(&self) -> &str {
        &self.state
    }
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl State {
    fn from_tokens(tokens: &[&str], index: &Index) -> Result<Self> {
        let name_index = index
            .node_name
            .ok_or_else(|| anyhow!("no node name index"))?;
        let name = tokens[name_index].into();

        let state_index =
            index.state.ok_or_else(|| anyhow!("no state index"))?;
        let state = tokens[state_index].into();

        Ok(Self { name, state })
    }
}

#[derive(Debug, Default)]
struct Index {
    node_name: Option<usize>,
    state: Option<usize>,
}

fn header_to_index(tokens: &[&str], index: &mut Index) {
    for (i, token) in tokens.iter().enumerate() {
        match *token {
            "nodeName" => index.node_name = Some(i),
            "state" => index.state = Some(i),
            _ => {}
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
    fn parse() {
        let input = include_str!("state-example.in");

        let fs = States::from_reader(input.as_bytes()).unwrap();
        let mut fs = fs.0.into_iter();

        assert_eq!(
            fs.next(),
            Some(State {
                name: "filer1".into(),
                state: "active".into(),
            })
        );

        assert_eq!(fs.next(), None);
    }
}
