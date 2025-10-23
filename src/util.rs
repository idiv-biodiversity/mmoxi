//! Utilities.

use std::str::FromStr;

use anyhow::{Error, Result, anyhow};

/// Boolean type as used by various `mm* -Y` output.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum MMBool {
    /// False.
    No,

    /// True.
    Yes,
}

impl MMBool {
    /// Returns the `bool` representation.
    #[must_use]
    pub const fn as_bool(&self) -> bool {
        match self {
            Self::No => false,
            Self::Yes => true,
        }
    }
}

impl FromStr for MMBool {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "no" | "0" => Ok(Self::No),
            "yes" | "1" => Ok(Self::Yes),
            unknown => Err(anyhow!("unknown boolean value: {unknown}")),
        }
    }
}
