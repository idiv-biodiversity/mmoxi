//! `mmhealth` parsing.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::process::Command;

use anyhow::{Context, Result, anyhow};

/// Returns the `mmhealth` status at node level.
///
/// # Errors
///
/// Returns an error if running `mmhealth` fails or if parsing its output fails.
pub fn node() -> Result<NodeStates> {
    let mut cmd = Command::new("mmhealth");
    cmd.arg("node");
    cmd.arg("show");
    cmd.arg("-Y");

    let output = cmd
        .output()
        .with_context(|| format!("error running: {cmd:?}"))?;

    let data = NodeStates::from_reader(output.stdout.as_slice())?;

    Ok(data)
}

/// Node status identifier.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NodeIdentifier {
    component: String,
    entityname: String,
    entitytype: String,
}

/// Status at node level.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct NodeStates {
    states: Vec<NodeState>,
}

impl NodeStates {
    fn from_reader<Input: BufRead>(input: Input) -> Result<Self> {
        let mut event_index = NodeEventIndex::default();
        let mut state_index = NodeStateIndex::default();
        let mut data = Self::default();

        for line in input.lines() {
            let line = line?;

            let tokens = line.split(':').collect::<Vec<_>>();

            if tokens[1] == "State" {
                if tokens[2] == "HEADER" {
                    state_index = NodeStateIndex::default();
                    state_index.with_tokens(&tokens);
                } else {
                    let new = NodeState::from_tokens(&tokens, &state_index)?;
                    data.states.push(new);
                }
            }

            if tokens[1] == "Event" {
                if tokens[2] == "HEADER" {
                    event_index = NodeEventIndex::default();
                    event_index.with_tokens(&tokens);
                } else {
                    let (id, event) =
                        NodeEvent::from_tokens(&tokens, &event_index)?;

                    let needle = data.states.iter_mut().find(|state| {
                        state.id.component == id.component
                            && state.id.entityname == id.entityname
                            && state.id.entitytype == id.entitytype
                    });

                    if let Some(state) = needle {
                        state.events.push(event);
                    }
                }
            }
        }

        Ok(data)
    }
}

/// Single node event.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NodeEvent {
    event: String,
    arguments: Option<String>,
    identifier: Option<String>,
    message: String,
}

/// Single status at node level.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NodeState {
    id: NodeIdentifier,
    status: String,
    events: Vec<NodeEvent>,
}

// ----------------------------------------------------------------------------
// boiler-platy parsing
// ----------------------------------------------------------------------------

impl NodeState {
    fn from_tokens(tokens: &[&str], index: &NodeStateIndex) -> Result<Self> {
        let component = index
            .component
            .ok_or_else(|| anyhow!("no component index"))?;
        let component = tokens[component].into();

        let entityname = index
            .entityname
            .ok_or_else(|| anyhow!("no entityname index"))?;
        let entityname = tokens[entityname].into();

        let entitytype = index
            .entitytype
            .ok_or_else(|| anyhow!("no entitytype index"))?;
        let entitytype = tokens[entitytype].into();

        let status = index.status.ok_or_else(|| anyhow!("no status index"))?;
        let status = tokens[status].into();

        Ok(Self {
            id: NodeIdentifier {
                component,
                entityname,
                entitytype,
            },
            status,
            events: vec![],
        })
    }
}

#[derive(Debug, Default)]
struct NodeStateIndex {
    component: Option<usize>,
    entityname: Option<usize>,
    entitytype: Option<usize>,
    status: Option<usize>,
}

impl NodeStateIndex {
    fn with_tokens(&mut self, tokens: &[&str]) {
        for (i, token) in tokens.iter().enumerate() {
            match *token {
                "component" => self.component = Some(i),
                "entityname" => self.entityname = Some(i),
                "entitytype" => self.entitytype = Some(i),
                "status" => self.status = Some(i),
                _ => {}
            }
        }
    }
}

impl NodeEvent {
    fn from_tokens(
        tokens: &[&str],
        index: &NodeEventIndex,
    ) -> Result<(NodeIdentifier, Self)> {
        let component = index
            .component
            .ok_or_else(|| anyhow!("no component index"))?;
        let component = tokens[component].into();

        let entityname = index
            .entityname
            .ok_or_else(|| anyhow!("no entityname index"))?;
        let entityname = tokens[entityname].into();

        let entitytype = index
            .entitytype
            .ok_or_else(|| anyhow!("no entitytype index"))?;
        let entitytype = tokens[entitytype].into();

        let event = index.event.ok_or_else(|| anyhow!("no event index"))?;
        let event = tokens[event].into();

        let arguments = index
            .arguments
            .ok_or_else(|| anyhow!("no arguments index"))?;
        let arguments = tokens[arguments];
        let arguments = (!arguments.is_empty()).then_some(arguments.into());

        let identifier = index
            .identifier
            .ok_or_else(|| anyhow!("no identifier index"))?;
        let identifier = tokens[identifier];
        let identifier = (!identifier.is_empty()).then_some(identifier.into());

        let message =
            index.message.ok_or_else(|| anyhow!("no message index"))?;
        let message = tokens[message].into();

        Ok((
            NodeIdentifier {
                component,
                entityname,
                entitytype,
            },
            Self {
                event,
                arguments,
                identifier,
                message,
            },
        ))
    }
}

#[derive(Debug, Default)]
struct NodeEventIndex {
    component: Option<usize>,
    entityname: Option<usize>,
    entitytype: Option<usize>,
    event: Option<usize>,
    arguments: Option<usize>,
    identifier: Option<usize>,
    message: Option<usize>,
}

impl NodeEventIndex {
    fn with_tokens(&mut self, tokens: &[&str]) {
        for (i, token) in tokens.iter().enumerate() {
            match *token {
                "component" => self.component = Some(i),
                "entityname" => self.entityname = Some(i),
                "entitytype" => self.entitytype = Some(i),
                "event" => self.event = Some(i),
                "arguments" => self.arguments = Some(i),
                "identifier" => self.identifier = Some(i),
                "message" => self.message = Some(i),
                _ => {}
            }
        }
    }
}

// ----------------------------------------------------------------------------
// prometheus
// ----------------------------------------------------------------------------

impl crate::prom::ToText for NodeStates {
    fn to_prom(&self, output: &mut impl Write) -> Result<()> {
        let mut status: BTreeMap<&str, u64> = BTreeMap::new();

        for state in &self.states {
            *status.entry(&state.status).or_default() += 1;
        }

        writeln!(output, "# HELP gpfs_health_node GPFS mmhealth node show.")?;
        writeln!(output, "# TYPE gpfs_health_node gauge")?;

        for (name, count) in status {
            write!(output, "gpfs_health_node{{")?;
            write!(output, "status=\"{name}\"")?;
            write!(output, "}} {count}")?;
            writeln!(output)?;
        }

        Ok(())
    }
}

// ----------------------------------------------------------------------------
// tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::prom::ToText;

    use super::*;

    #[allow(clippy::too_many_lines)]
    #[test]
    fn parse_node() {
        let input = include_str!("health-node-example.in");

        let data = NodeStates::from_reader(input.as_bytes()).unwrap();

        assert_eq!(
            data.states,
            vec![
                NodeState {
                    id: NodeIdentifier {
                        component: "NODE".into(),
                        entityname: "frontend2-ib0".into(),
                        entitytype: "NODE".into(),
                    },
                    status: "TIPS".into(),
                    events: vec![],
                },
                NodeState {
                    id: NodeIdentifier {
                        component: "GPFS".into(),
                        entityname: "frontend2-ib0".into(),
                        entitytype: "NODE".into(),
                    },
                    status: "TIPS".into(),
                    events: vec![NodeEvent {
                        event: "unexpected_operating_system".into(),
                        arguments: None,
                        identifier: None,
                        message:
                            "An unexpected operating system was detected."
                                .into()
                    }]
                },
                NodeState {
                    id: NodeIdentifier {
                        component: "NETWORK".into(),
                        entityname: "frontend2-ib0".into(),
                        entitytype: "NODE".into(),
                    },
                    status: "HEALTHY".into(),
                    events: vec![],
                },
                NodeState {
                    id: NodeIdentifier {
                        component: "NETWORK".into(),
                        entityname: "ib0".into(),
                        entitytype: "NIC".into(),
                    },
                    status: "HEALTHY".into(),
                    events: vec![],
                },
                NodeState {
                    id: NodeIdentifier {
                        component: "NETWORK".into(),
                        entityname: "hfi1_0/1".into(),
                        entitytype: "IB_RDMA".into(),
                    },
                    status: "HEALTHY".into(),
                    events: vec![],
                },
                NodeState {
                    id: NodeIdentifier {
                        component: "FILESYSTEM".into(),
                        entityname: "frontend2-ib0".into(),
                        entitytype: "NODE".into(),
                    },
                    status: "TIPS".into(),
                    events: vec![],
                },
                NodeState {
                    id: NodeIdentifier {
                        component: "FILESYSTEM".into(),
                        entityname: "gpfs1".into(),
                        entitytype: "FILESYSTEM".into(),
                    },
                    status: "TIPS".into(),
                    events: vec![NodeEvent {
                        event: "ill_unbalanced_fs".into(),
                        arguments: Some("gpfs1".into()),
                        identifier: Some("gpfs1".into()),
                        message:
                            "The file system gpfs1 is not properly balanced."
                                .into()
                    }]
                },
                NodeState {
                    id: NodeIdentifier {
                        component: "PERFMON".into(),
                        entityname: "frontend2-ib0".into(),
                        entitytype: "NODE".into(),
                    },
                    status: "HEALTHY".into(),
                    events: vec![],
                },
                NodeState {
                    id: NodeIdentifier {
                        component: "THRESHOLD".into(),
                        entityname: "frontend2-ib0".into(),
                        entitytype: "NODE".into(),
                    },
                    status: "HEALTHY".into(),
                    events: vec![],
                }
            ]
        );

        let mut output = vec![];
        data.to_prom(&mut output).unwrap();

        let metrics = std::str::from_utf8(output.as_slice()).unwrap();

        let expected = include_str!("health-node-example.prom");
        assert_eq!(metrics, expected);
    }
}
