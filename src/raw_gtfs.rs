use crate::objects::*;
use chrono::Utc;
use failure::format_err;
use failure::Error;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

/// Data structure that map the GTFS csv with little intelligence
pub struct RawGtfs {
    pub read_duration: i64,
    pub calendar: Result<Vec<Calendar>, Error>,
    pub calendar_dates: Result<Vec<CalendarDate>, Error>,
    pub stops: Result<Vec<Stop>, Error>,
    pub routes: Result<Vec<Route>, Error>,
    pub trips: Result<Vec<RawTrip>, Error>,
    pub agencies: Result<Vec<Agency>, Error>,
    pub shapes: Option<Result<Vec<Shape>, Error>>,
    pub fare_attributes: Option<Result<Vec<FareAttribute>, Error>>,
    pub feed_info: Option<Result<Vec<FeedInfo>, Error>>,
    pub stop_times: Result<Vec<RawStopTime>, Error>,
}

fn read_objs<T, O>(reader: T) -> Result<Vec<O>, Error>
where
    for<'de> O: Deserialize<'de>,
    T: std::io::Read,
{
    Ok(csv::Reader::from_reader(reader)
        .deserialize()
        .collect::<Result<_, _>>()?)
}

fn read_objs_from_path<O>(path: std::path::PathBuf) -> Result<Vec<O>, Error>
where
    for<'de> O: Deserialize<'de>,
{
    File::open(path)
        .map_err(|e| format_err!("Could not find file: {}", e))
        .and_then(read_objs)
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
        .map(|i| read_objs(archive.by_index(*i)?))
        .unwrap_or_else(|| Err(format_err!("Could not find file {}", file_name)))
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

    pub fn new(path: &str) -> Result<Self, Error> {
        let now = Utc::now();
        let p = Path::new(path);

        // Thoses files are not mandatory
        // We use None if they don’t exist, not an Error
        let shapes_file = File::open(p.join("shapes.txt")).ok();
        let fare_attributes_file = File::open(p.join("fare_attributes.txt")).ok();
        let feed_info_file = File::open(p.join("feed_info.txt")).ok();

        Ok(Self {
            trips: read_objs_from_path(p.join("trips.txt")),
            calendar: read_objs_from_path(p.join("calendar.txt")),
            calendar_dates: read_objs_from_path(p.join("calendar_dates.txt")),
            stops: read_objs_from_path(p.join("stops.txt")),
            routes: read_objs_from_path(p.join("routes.txt")),
            stop_times: read_objs_from_path(p.join("stop_times.txt")),
            agencies: read_objs_from_path(p.join("agency.txt")),
            shapes: shapes_file.map(read_objs),
            fare_attributes: fare_attributes_file.map(read_objs),
            feed_info: feed_info_file.map(read_objs),
            read_duration: Utc::now().signed_duration_since(now).num_milliseconds(),
        })
    }

    pub fn from_zip(file: &str) -> Result<Self, Error> {
        let reader = File::open(file)?;
        Self::from_reader(reader)
    }

    #[cfg(feature = "read-url")]
    pub fn from_url(url: &str) -> Result<Self, Error> {
        use std::io::Read;
        let mut res = reqwest::get(url)?;
        let mut body = Vec::new();
        res.read_to_end(&mut body)?;
        let cursor = std::io::Cursor::new(body);
        Self::from_reader(cursor)
    }
    pub fn from_reader<T: std::io::Read + std::io::Seek>(reader: T) -> Result<Self, Error> {
        let now = Utc::now();
        let mut archive = zip::ZipArchive::new(reader)?;
        let mut file_mapping = HashMap::new();

        for i in 0..archive.len() {
            let archive_file = archive.by_index(i)?;

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
                if archive_file.name().ends_with(gtfs_file) {
                    file_mapping.insert(gtfs_file, i);
                    break;
                }
            }
        }

        Ok(Self {
            agencies: read_file(&file_mapping, &mut archive, "agency.txt"),
            calendar: read_file(&file_mapping, &mut archive, "calendar.txt"),
            calendar_dates: read_file(&file_mapping, &mut archive, "calendar_dates.txt"),
            routes: read_file(&file_mapping, &mut archive, "routes.txt"),
            stops: read_file(&file_mapping, &mut archive, "stops.txt"),
            stop_times: read_file(&file_mapping, &mut archive, "stop_times.txt"),
            trips: read_file(&file_mapping, &mut archive, "trips.txt"),
            fare_attributes: file_mapping
                .get(&"fare_attributes.txt")
                .map(|i| read_objs(archive.by_index(*i)?)),
            feed_info: file_mapping
                .get(&"feed_info.txt")
                .map(|i| read_objs(archive.by_index(*i)?)),
            shapes: file_mapping
                .get(&"shapes.txt")
                .map(|i| read_objs(archive.by_index(*i)?)),
            read_duration: Utc::now().signed_duration_since(now).num_milliseconds(),
        })
    }
}
