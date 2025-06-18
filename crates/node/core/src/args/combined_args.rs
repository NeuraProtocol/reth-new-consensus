//! Combined CLI arguments for all optional protocols

use clap::Args;
use crate::args::{RessArgs, NarwhalBullsharkArgs};

/// Combined arguments for all optional protocols/consensus mechanisms
#[derive(Debug, Clone, Args, PartialEq, Eq)]
pub struct CombinedProtocolArgs {
    /// Ress subprotocol arguments
    #[command(flatten)]
    pub ress: RessArgs,

    /// Narwhal + Bullshark consensus arguments
    #[command(flatten)]
    pub narwhal_bullshark: NarwhalBullsharkArgs,
}

impl Default for CombinedProtocolArgs {
    fn default() -> Self {
        Self {
            ress: RessArgs::default(),
            narwhal_bullshark: NarwhalBullsharkArgs::default(),
        }
    }
} 