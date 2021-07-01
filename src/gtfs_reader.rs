use crate::{Error, Gtfs, RawGtfs};
use std::convert::TryFrom;
use std::path::Path;

/// Allows to parameterize how the parsing library behaves
///
/// ```
///let gtfs = gtfs_structures::GtfsReader::default()
///    .without_stop_times()
///    .read("fixtures/zips/gtfs.zip")?;
///assert_eq!(0, gtfs.trips.get("trip1").unwrap().stop_times.len());
/// # Ok::<(), gtfs_structures::error::Error>(())
///```
#[derive(Derivative)]
#[derivative(Default)]
pub struct GtfsReader {
    /// [crate::objects::StopTime] are very large and not always needed. This allows to skip reading them
    #[derivative(Default(value = "true"))]
    pub read_stop_times: bool,
}

impl GtfsReader {
    /// Configures the reader to not read the stop times
    ///
    /// This can be useful to save time and memory with large datasets when the timetable are not needed
    /// Returns Self and can be chained
    pub fn without_stop_times(&mut self) -> &mut Self {
        self.read_stop_times = false;
        self
    }

    /// Reads from an url (if starts with `"http"`), or a local path (either a directory or zipped file)
    ///
    /// To read from an url, build with read-url feature
    /// See also [Gtfs::from_url] and [Gtfs::from_path] if you don’t want the library to guess
    pub fn read(&self, gtfs: &str) -> Result<Gtfs, Error> {
        RawGtfs::new_params(gtfs, self).and_then(Gtfs::try_from)
    }

    /// Reads the raw GTFS from a local zip archive or local directory
    pub fn raw_from_path<P>(&self, path: P) -> Result<RawGtfs, Error>
    where
        P: AsRef<Path> + std::fmt::Display,
    {
        RawGtfs::from_path_params(path, self)
    }

    /// Reads the raw GTFS from a local zip archive or local directory
    pub fn from_path<P>(&self, path: P) -> Result<Gtfs, Error>
    where
        P: AsRef<Path> + std::fmt::Display,
    {
        RawGtfs::from_path_params(path, self).and_then(Gtfs::try_from)
    }

    /// Reads the GTFS from a remote url
    ///
    /// The library must be built with the read-url feature
    #[cfg(feature = "read-url")]
    pub fn from_url<U: reqwest::IntoUrl>(&self, url: U) -> Result<Gtfs, Error> {
        RawGtfs::from_url_params(url, self).and_then(Gtfs::try_from)
    }

    /// Asynchronously reads the GTFS from a remote url
    ///
    /// The library must be built with the read-url feature
    #[cfg(feature = "read-url")]
    pub async fn from_url_async<U: reqwest::IntoUrl>(&self, url: U) -> Result<Gtfs, Error> {
        RawGtfs::from_url_async_params(url, self)
            .await
            .and_then(Gtfs::try_from)
    }
}
