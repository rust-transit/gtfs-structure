#[macro_use]
extern crate derivative;
#[macro_use]
extern crate serde_derive;

mod error;
mod gtfs;
pub(crate) mod objects;
mod raw_gtfs;

#[cfg(test)]
mod tests;

pub use error::Error;
pub use gtfs::Gtfs;
pub use objects::*;
pub use raw_gtfs::RawGtfs;
