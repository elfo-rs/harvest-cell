#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

pub use self::{
    cell::{Borrowed, HarvestCell, HarvestHandle, Owned, Storage, Writer},
    harvester::{HarvestResult, Harvester, IntoHarvestHandles},
};

mod barrier;
mod cell;
mod harvester;
mod raw;
mod sync;
