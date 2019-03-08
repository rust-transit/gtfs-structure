use chrono::Utc;
use failure::Error;
use failure::ResultExt;
use serde::Deserialize;
use std::fs::File;
use std::path::Path;

use crate::objects::*;

/// Data structure that map the GTFS csv with little intelligence
#[derive(Default)]
pub struct RawGtfs {
    pub read_duration: i64,
    pub calendar: Vec<Calendar>,
    pub calendar_dates: Vec<CalendarDate>,
    pub stops: Vec<Stop>,
    pub routes: Vec<Route>,
    pub trips: Vec<RawTrip>,
    pub agencies: Vec<Agency>,
    pub shapes: Vec<Shape>,
    pub fare_attributes: Vec<FareAttribute>,
    pub feed_info: Vec<FeedInfo>,
    pub stop_times: Vec<RawStopTime>,
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

impl RawGtfs {
    pub fn print_stats(&self) {
        println!("GTFS data:");
        println!("  Read in {} ms", self.read_duration);
        println!("  Stops: {}", self.stops.len());
        println!("  Routes: {}", self.routes.len());
        println!("  Trips: {}", self.trips.len());
        println!("  Agencies: {}", self.agencies.len());
        println!("  Shapes: {}", self.shapes.len());
        println!("  Fare attributes: {}", self.fare_attributes.len());
        println!("  Feed info: {}", self.feed_info.len());
    }

    pub fn new(path: &str) -> Result<Self, Error> {
        let now = Utc::now();
        let p = Path::new(path);
        let trips_file = File::open(p.join("trips.txt"))?;
        let calendar_file = File::open(p.join("calendar.txt"))?;
        let stops_file = File::open(p.join("stops.txt"))?;
        let calendar_dates_file = File::open(p.join("calendar_dates.txt"))?;
        let routes_file = File::open(p.join("routes.txt"))?;
        let stop_times_file = File::open(p.join("stop_times.txt"))?;
        let agencies_file = File::open(p.join("agency.txt"))?;
        let shapes_file = File::open(p.join("shapes.txt")).ok();
        let fare_attributes_file = File::open(p.join("fare_attributes.txt")).ok();
        let feed_info_file = File::open(p.join("feed_info.txt")).ok();

        let mut gtfs = Self::default();

        gtfs.trips = read_objs(trips_file)?;
        gtfs.calendar = read_objs(calendar_file)?;
        gtfs.calendar_dates = read_objs(calendar_dates_file)?;
        gtfs.stops = read_objs(stops_file)?;
        gtfs.routes = read_objs(routes_file)?;
        gtfs.stop_times = read_objs(stop_times_file)?;
        gtfs.agencies = read_objs(agencies_file)?;
        if let Some(s_file) = shapes_file {
            gtfs.shapes = read_objs(s_file)?;
        }
        if let Some(f_a_file) = fare_attributes_file {
            gtfs.fare_attributes = read_objs(f_a_file)?;
        }
        if let Some(f_i_file) = feed_info_file {
            gtfs.feed_info = read_objs(f_i_file)?;
        }

        gtfs.read_duration = Utc::now().signed_duration_since(now).num_milliseconds();
        Ok(gtfs)
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
        let mut gtfs = Self::default();
        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            if file.name().ends_with("calendar.txt") {
                gtfs.calendar = read_objs(file)
                    .with_context(|e| format!("Error reading calendar.txt : {}", e))?;
            } else if file.name().ends_with("stops.txt") {
                gtfs.stops =
                    read_objs(file).with_context(|e| format!("Error reading stops.txt : {}", e))?;
            } else if file.name().ends_with("calendar_dates.txt") {
                gtfs.calendar_dates = read_objs(file)
                    .with_context(|e| format!("Error reading calendar_dates.txt : {}", e))?;
            } else if file.name().ends_with("routes.txt") {
                gtfs.routes = read_objs(file)
                    .with_context(|e| format!("Error reading routes.txt : {}", e))?;
            } else if file.name().ends_with("trips.txt") {
                gtfs.trips =
                    read_objs(file).with_context(|e| format!("Error reading trips.txt : {}", e))?;
            } else if file.name().ends_with("stop_times.txt") {
                gtfs.stop_times = read_objs(file)
                    .with_context(|e| format!("Error reading stop_times.txt : {}", e))?;
            } else if file.name().ends_with("agency.txt") {
                gtfs.agencies = read_objs(file)
                    .with_context(|e| format!("Error reading agency.txt : {}", e))?;
            } else if file.name().ends_with("shapes.txt") {
                gtfs.shapes = read_objs(file)
                    .with_context(|e| format!("Error reading shapes.txt : {}", e))?;
            } else if file.name().ends_with("fare_attributes.txt") {
                gtfs.fare_attributes = read_objs(file)
                    .with_context(|e| format!("Error reading fare_attributes.txt : {}", e))?;
            } else if file.name().ends_with("feed_info.txt") {
                gtfs.feed_info = read_objs(file)
                    .with_context(|e| format!("Error reading feed_info.txt : {}", e))?;
            }
        }
        gtfs.read_duration = Utc::now().signed_duration_since(now).num_milliseconds();
        Ok(gtfs)
    }
}
