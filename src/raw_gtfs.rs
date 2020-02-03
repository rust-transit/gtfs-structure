use crate::objects::*;
use anyhow::{anyhow, Context, Error};
use chrono::Utc;
use serde::Deserialize;
use sha2::digest::Digest;
use sha2::Sha256;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Data structure that map the GTFS csv with little intelligence
pub struct RawGtfs {
    pub read_duration: i64,
    pub calendar: Option<Result<Vec<Calendar>, Error>>,
    pub calendar_dates: Option<Result<Vec<CalendarDate>, Error>>,
    pub stops: Result<Vec<Stop>, Error>,
    pub routes: Result<Vec<Route>, Error>,
    pub trips: Result<Vec<RawTrip>, Error>,
    pub agencies: Result<Vec<Agency>, Error>,
    pub shapes: Option<Result<Vec<Shape>, Error>>,
    pub fare_attributes: Option<Result<Vec<FareAttribute>, Error>>,
    pub feed_info: Option<Result<Vec<FeedInfo>, Error>>,
    pub stop_times: Result<Vec<RawStopTime>, Error>,
    pub files: Vec<String>,
    pub sha256: Option<String>,
}

fn read_objs<T, O>(mut reader: T, file_name: &str) -> Result<Vec<O>, Error>
where
    for<'de> O: Deserialize<'de>,
    T: std::io::Read,
{
    let mut bom = [0; 3];
    reader.read_exact(&mut bom)?;

    let chained = if bom != [0xefu8, 0xbbu8, 0xbfu8] {
        bom.chain(reader)
    } else {
        [].chain(reader)
    };

    Ok(csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(chained)
        .deserialize()
        .collect::<Result<_, _>>()
        .context(format!("error while reading {}", file_name))?)
}

fn read_objs_from_path<O>(path: std::path::PathBuf) -> Result<Vec<O>, Error>
where
    for<'de> O: Deserialize<'de>,
{
    let file_name = path
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or_else(|| "invalid_file_name")
        .to_string();
    File::open(path)
        .map_err(|e| anyhow!("Could not find file: {}", e))
        .and_then(|r| read_objs(r, &file_name))
}

fn read_objs_from_optional_path<O>(
    dir_path: &std::path::Path,
    file_name: &str,
) -> Option<Result<Vec<O>, Error>>
where
    for<'de> O: Deserialize<'de>,
{
    File::open(dir_path.join(file_name))
        .ok()
        .map(|r| read_objs(r, file_name))
}

fn read_file<O, T>(
    file_mapping: &HashMap<&&str, usize>,
    archive: &mut zip::ZipArchive<T>,
    file_name: &str,
) -> Result<Vec<O>, Error>
where
    for<'de> O: Deserialize<'de>,
    T: std::io::Read + std::io::Seek,
{
    file_mapping
        .get(&file_name)
        .map(|i| read_objs(archive.by_index(*i)?, file_name))
        .unwrap_or_else(|| Err(anyhow!("Could not find file {}", file_name)))
}

fn read_optional_file<O, T>(
    file_mapping: &HashMap<&&str, usize>,
    archive: &mut zip::ZipArchive<T>,
    file_name: &str,
) -> Option<Result<Vec<O>, Error>>
where
    for<'de> O: Deserialize<'de>,
    T: std::io::Read + std::io::Seek,
{
    file_mapping
        .get(&file_name)
        .map(|i| read_objs(archive.by_index(*i)?, file_name))
}

fn mandatory_file_summary<T>(objs: &Result<Vec<T>, Error>) -> String {
    match objs {
        Ok(vec) => format!("{} objects", vec.len()),
        Err(e) => format!("Could not read {}", e),
    }
}

fn optional_file_summary<T>(objs: &Option<Result<Vec<T>, Error>>) -> String {
    match objs {
        Some(objs) => mandatory_file_summary(objs),
        None => "File not present".to_string(),
    }
}

impl RawGtfs {
    /// Prints on stdout some basic statistics about the GTFS file
    pub fn print_stats(&self) {
        println!("GTFS data:");
        println!("  Read in {} ms", self.read_duration);
        println!("  Stops: {}", mandatory_file_summary(&self.stops));
        println!("  Routes: {}", mandatory_file_summary(&self.routes));
        println!("  Trips: {}", mandatory_file_summary(&self.trips));
        println!("  Agencies: {}", mandatory_file_summary(&self.agencies));
        println!("  Stop times: {}", mandatory_file_summary(&self.stop_times));
        println!("  Shapes: {}", optional_file_summary(&self.shapes));
        println!("  Fares: {}", optional_file_summary(&self.fare_attributes));
        println!("  Feed info: {}", optional_file_summary(&self.feed_info));
    }

    /// Reads from an url (if starts with http), or a local path (either a directory or zipped file)
    /// To read from an url, build with read-url feature
    /// See also RawGtfs::from_url and RawGtfs::from_path if you don’t want the library to guess
    #[cfg(feature = "read-url")]
    pub fn new(gtfs: &str) -> Result<Self, Error> {
        if gtfs.starts_with("http") {
            Self::from_url(gtfs)
        } else {
            Self::from_path(gtfs)
        }
    }

    #[cfg(not(feature = "read-url"))]
    pub fn new(gtfs_source: &str) -> Result<Self, Error> {
        Self::from_path(gtfs_source)
    }

    /// Reads the raw GTFS from a local zip archive or local directory
    pub fn from_path<P>(path: P) -> Result<Self, Error>
    where
        P: AsRef<Path> + std::fmt::Display,
    {
        let p = path.as_ref();
        if p.is_file() {
            let reader = File::open(p)?;
            Self::from_reader(reader)
        } else if p.is_dir() {
            Self::from_directory(p)
        } else {
            Err(anyhow!(
                "Could not read GTFS: {} is neither a file nor a directory",
                path
            ))
        }
    }

    fn from_directory(p: &std::path::Path) -> Result<Self, Error> {
        let now = Utc::now();
        // Thoses files are not mandatory
        // We use None if they don’t exist, not an Error
        let files = std::fs::read_dir(p)?
            .filter_map(|d| d.ok().and_then(|p| p.path().to_str().map(|s| s.to_owned())))
            .collect();

        Ok(Self {
            trips: read_objs_from_path(p.join("trips.txt")),
            calendar: read_objs_from_optional_path(&p, "calendar.txt"),
            calendar_dates: read_objs_from_optional_path(&p, "calendar_dates.txt"),
            stops: read_objs_from_path(p.join("stops.txt")),
            routes: read_objs_from_path(p.join("routes.txt")),
            stop_times: read_objs_from_path(p.join("stop_times.txt")),
            agencies: read_objs_from_path(p.join("agency.txt")),
            shapes: read_objs_from_optional_path(&p, "shapes.txt"),
            fare_attributes: read_objs_from_optional_path(&p, "fare_attributes.txt"),
            feed_info: read_objs_from_optional_path(&p, "feed_info.txt"),
            read_duration: Utc::now().signed_duration_since(now).num_milliseconds(),
            files,
            sha256: None,
        })
    }

    /// Reads the raw GTFS from a remote url
    /// The library must be built with the read-url feature
    #[cfg(feature = "read-url")]
    pub fn from_url<U: reqwest::IntoUrl>(url: U) -> Result<Self, Error> {
        let mut res = reqwest::get(url)?;
        let mut body = Vec::new();
        res.read_to_end(&mut body)?;
        let cursor = std::io::Cursor::new(body);
        Self::from_reader(cursor)
    }

    /// Non-blocking read the raw GTFS from a remote url
    /// The library must be built with the read-url feature
    #[cfg(feature = "read-url")]
    pub fn from_url_async(url: &str) -> impl futures::Future<Item = Self, Error = Error> {
        use futures::{Future, Stream};
        let client = reqwest::r#async::Client::new();
        client
            .get(url)
            .send()
            .from_err()
            .and_then(|res| {
                res.into_body().map_err(Error::from).fold(
                    bytes::BytesMut::new(),
                    move |mut body, chunk| {
                        body.extend_from_slice(&chunk);
                        Ok::<_, Error>(body)
                    },
                )
            })
            .and_then(move |body| Self::from_reader(std::io::Cursor::new(body)))
    }

    pub fn from_reader<T: std::io::Read + std::io::Seek>(reader: T) -> Result<Self, Error> {
        let now = Utc::now();
        let mut hasher = Sha256::new();
        let mut buf_reader = std::io::BufReader::new(reader);
        let _n = std::io::copy(&mut buf_reader, &mut hasher)?;
        let hash = hasher.result();
        let mut archive = zip::ZipArchive::new(buf_reader)?;
        let mut file_mapping = HashMap::new();
        let mut files = Vec::new();

        for i in 0..archive.len() {
            let archive_file = archive.by_index(i)?;
            files.push(archive_file.name().to_owned());

            for gtfs_file in &[
                "agency.txt",
                "calendar.txt",
                "calendar_dates.txt",
                "routes.txt",
                "stops.txt",
                "stop_times.txt",
                "trips.txt",
                "fare_attributes.txt",
                "feed_info.txt",
                "shapes.txt",
            ] {
                let path = std::path::Path::new(archive_file.name());
                if path.file_name() == Some(std::ffi::OsStr::new(gtfs_file)) {
                    file_mapping.insert(gtfs_file, i);
                    break;
                }
            }
        }

        Ok(Self {
            agencies: read_file(&file_mapping, &mut archive, "agency.txt"),
            calendar: read_optional_file(&file_mapping, &mut archive, "calendar.txt"),
            calendar_dates: read_optional_file(&file_mapping, &mut archive, "calendar_dates.txt"),
            routes: read_file(&file_mapping, &mut archive, "routes.txt"),
            stops: read_file(&file_mapping, &mut archive, "stops.txt"),
            stop_times: read_file(&file_mapping, &mut archive, "stop_times.txt"),
            trips: read_file(&file_mapping, &mut archive, "trips.txt"),
            fare_attributes: read_optional_file(&file_mapping, &mut archive, "fare_attributes.txt"),
            feed_info: read_optional_file(&file_mapping, &mut archive, "feed_info.txt"),
            shapes: read_optional_file(&file_mapping, &mut archive, "shapes.txt"),
            read_duration: Utc::now().signed_duration_since(now).num_milliseconds(),
            files,
            sha256: Some(format!("{:x}", hash)),
        })
    }
}
