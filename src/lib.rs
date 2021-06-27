//! The [General Transit Feed Specification](https://gtfs.org/) (GTFS) is a commonly used model to represent public transit data.
//!
//! This crates brings [serde](https://serde.rs) structures of this model and helpers to read GTFS files.

#[macro_use]
extern crate derivative;
#[macro_use]
extern crate serde_derive;

pub mod error;
mod gtfs;
pub(crate) mod objects;
mod raw_gtfs;

#[cfg(test)]
mod tests;

pub use error::Error;
pub use gtfs::Gtfs;
pub use objects::*;
pub use raw_gtfs::RawGtfs;
