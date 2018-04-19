extern crate chrono;
extern crate csv;
extern crate failure;
extern crate regex;
extern crate reqwest;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate zip;

use std::io::Read;
use std::fs::File;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use chrono::prelude::*;
use serde::de::{self, Deserialize, Deserializer};
use chrono::Duration;
use failure::Error;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum LocationType {
    StopPoint = 0,
    StopArea = 1,
    StationEntrance = 2,
}

#[derive(Debug, Deserialize)]
pub struct Calendar {
    #[serde(rename = "service_id")]
    id: String,
    #[serde(deserialize_with = "deserialize_bool")]
    monday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    tuesday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    wednesday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    thursday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    friday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    saturday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    sunday: bool,
    #[serde(deserialize_with = "deserialize_date")]
    pub start_date: NaiveDate,
    #[serde(deserialize_with = "deserialize_date")]
    pub end_date: NaiveDate,
}

impl Calendar {
    pub fn valid_weekday(&self, date: NaiveDate) -> bool {
        match date.weekday() {
            Weekday::Mon => self.monday,
            Weekday::Tue => self.tuesday,
            Weekday::Wed => self.wednesday,
            Weekday::Thu => self.thursday,
            Weekday::Fri => self.friday,
            Weekday::Sat => self.saturday,
            Weekday::Sun => self.sunday,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CalendarDate {
    service_id: String,
    #[serde(deserialize_with = "deserialize_date")]
    date: NaiveDate,
    exception_type: u8,
}

#[derive(Debug, Deserialize)]
pub struct Stop {
    #[serde(rename = "stop_id")]
    pub id: String,
    pub stop_name: String,
    #[serde(deserialize_with = "deserialize_location_type", default = "default_location_type")]
    pub location_type: LocationType,
    pub parent_station: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StopTime {
    pub trip_id: String,
    #[serde(deserialize_with = "deserialize_time")]
    pub arrival_time: u16,
    #[serde(deserialize_with = "deserialize_time")]
    pub departure_time: u16,
    pub stop_id: String,
    stop_sequence: u32,
    pickup_type: Option<u8>,
    drop_off_type: Option<u8>,
}

#[derive(Debug, Deserialize)]
pub struct Route {
    #[serde(rename = "route_id")]
    id: String,
    route_short_name: String,
    route_long_name: String,
    route_type: u8,
}

#[derive(Debug, Deserialize)]
pub struct Trip {
    #[serde(rename = "trip_id")]
    pub id: String,
    pub service_id: String,
    pub route_id: String,
}

fn deserialize_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s, "%Y%m%d").map_err(serde::de::Error::custom)
}

pub fn parse_time(s: String) -> Result<u16, Error> {
    let v: Vec<&str> = s.split(':').collect();
    Ok(&v[0].parse()? * 3600u16 + &v[1].parse()? * 60u16 + &v[2].parse()?)
}

fn deserialize_time<'de, D>(deserializer: D) -> Result<u16, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    parse_time(s).map_err(de::Error::custom)
}

fn deserialize_location_type<'de, D>(deserializer: D) -> Result<LocationType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    Ok(match s.as_str() {
        "1" => LocationType::StopArea,
        "2" => LocationType::StationEntrance,
        _ => LocationType::StopPoint,
    })
}

fn default_location_type() -> LocationType {
    LocationType::StopPoint
}

pub struct Gtfs {
    pub read_duration: i64,
    pub calendar: HashMap<String, Calendar>,
    pub calendar_dates: HashMap<String, Vec<CalendarDate>>,
    pub stops: Vec<Stop>,
    pub routes: HashMap<String, Route>,
    pub trips: HashMap<String, Trip>,
    pub stop_times: Vec<StopTime>,
}

impl Gtfs {
    pub fn print_stats(&self) {
        println!("GTFS data:");
        println!("  Read in {} ms", self.read_duration);
        println!("  Stops: {}", self.stops.len());
        println!("  Routes: {}", self.routes.len());
        println!("  Trips: {}", self.trips.len());
        println!("  Stop Times: {}", self.stop_times.len());
    }

    fn empty() -> Gtfs {
        Gtfs {
            read_duration: 0,
            calendar: HashMap::new(),
            calendar_dates: HashMap::new(),
            stops: Vec::new(),
            routes: HashMap::new(),
            trips: HashMap::new(),
            stop_times: Vec::new()
        }
    }

    pub fn new(path: &str) -> Result<Gtfs, Error> {
        let now = Utc::now();
        let p = Path::new(path);
        let calendar_file = File::open(p.join("calendar.txt"))?;
        let stops_file = File::open(p.join("stops.txt"))?;
        let calendar_dates_file = File::open(p.join("calendar_dates.txt"))?;
        let routes_file = File::open(p.join("routes.txt"))?;
        let trips_file = File::open(p.join("trips.txt"))?;
        let stop_times_file = File::open(p.join("stop_times.txt"))?;

        Ok(Gtfs {
            calendar: Gtfs::read_calendars(calendar_file)?,
            stops: Gtfs::read_stops(stops_file)?,
            calendar_dates: Gtfs::read_calendar_dates(calendar_dates_file)?,
            routes: Gtfs::read_routes(routes_file)?,
            trips: Gtfs::read_trips(trips_file)?,
            stop_times: Gtfs::read_stop_times(stop_times_file)?,
            read_duration: Utc::now().signed_duration_since(now).num_milliseconds(),
        })
    }

    pub fn from_zip(file: &str) -> Result<Gtfs, Error> {
        let reader = File::open(file)?;
        Gtfs::from_reader(reader)
    }

    pub fn from_url(url: &str) -> Result<Gtfs, Error> {
        let mut res = reqwest::get(url)?;
        let mut body = Vec::new();
        res.read_to_end(&mut body)?;
        let cursor = std::io::Cursor::new(body);
        Gtfs::from_reader(cursor)
    }

    pub fn from_reader<T: std::io::Read + std::io::Seek>(reader: T) -> Result<Gtfs, Error> {
        let now = Utc::now();
        let mut archive = zip::ZipArchive::new(reader)?;
        let mut result = Gtfs::empty();
        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            if file.name().ends_with("calendar.txt") {
                result.calendar = Gtfs::read_calendars(file)?;
            } else if file.name().ends_with("stops.txt") {
                result.stops = Gtfs::read_stops(file)?;
            } else if file.name().ends_with("calendar_dates.txt") {
                result.calendar_dates = Gtfs::read_calendar_dates(file)?;
            } else if file.name().ends_with("routes.txt") {
                result.routes = Gtfs::read_routes(file)?;
            }  else if file.name().ends_with("trips.txt") {
                result.trips = Gtfs::read_trips(file)?;
            }  else if file.name().ends_with("stop_times.txt") {
                result.stop_times = Gtfs::read_stop_times(file)?;
            }
        }

        result.read_duration = Utc::now().signed_duration_since(now).num_milliseconds();
        Ok(result)
    }

    fn read_calendars<T: std::io::Read>(reader: T) -> Result<HashMap<String, Calendar>, Error> {
        let mut reader = csv::Reader::from_reader(reader);
        Ok(reader
            .deserialize()
            .map(|res| res.map(|e: Calendar| (e.id.to_owned(), e)))
            .collect::<Result<_, _>>()?)
    }

    fn read_calendar_dates<T: std::io::Read>(reader: T) -> Result<HashMap<String, Vec<CalendarDate>>, Error> {
        let mut reader = csv::Reader::from_reader(reader);
        let mut calendar_dates = HashMap::new();
        for result in reader.deserialize() {
            let record: CalendarDate = result?;
            let calendar_date = calendar_dates
                .entry(record.service_id.to_owned())
                .or_insert(Vec::new());
            calendar_date.push(record);
        }
        Ok(calendar_dates)
    }

    fn read_stops<T: std::io::Read>(reader: T) -> Result<Vec<Stop>, Error> {
        let mut reader = csv::Reader::from_reader(reader);
        Ok(reader.deserialize().collect::<Result<_, _>>()?)
    }

    fn read_routes<T: std::io::Read>(reader: T) -> Result<HashMap<String, Route>, Error> {
        let mut reader = csv::Reader::from_reader(reader);
        Ok(reader
            .deserialize()
            .map(|res| res.map(|e: Route| (e.id.to_owned(), e)))
            .collect::<Result<_, _>>()?)
    }

    fn read_trips<T: std::io::Read>(reader: T) -> Result<HashMap<String, Trip>, Error> {
        let mut reader = csv::Reader::from_reader(reader);
        Ok(reader
            .deserialize()
            .map(|res| res.map(|e: Trip| (e.id.to_owned(), e)))
            .collect::<Result<_, _>>()?)
    }

    fn read_stop_times<T: std::io::Read>(reader: T) -> Result<Vec<StopTime>, Error> {
        let mut reader = csv::Reader::from_reader(reader);
        let mut stop_times: Vec<StopTime> = reader.deserialize().collect::<Result<_, _>>()?;

        stop_times.sort_by(|a, b| {
            a.trip_id
                .cmp(&b.trip_id)
                .then(a.stop_sequence.cmp(&b.stop_sequence))
        });
        Ok(stop_times)
    }

    pub fn trip_days(&self, service_id: &String, start_date: NaiveDate) -> Vec<u16> {
        let mut result = Vec::new();

        // Handle services given by specific days and exceptions
        let mut removed_days = HashSet::new();
        for extra_day in self.calendar_dates
            .get(service_id)
            .iter()
            .flat_map(|e| e.iter())
        {
            let offset = extra_day.date.signed_duration_since(start_date).num_days();
            if offset >= 0 {
                if extra_day.exception_type == 1 {
                    result.push(offset as u16);
                } else if extra_day.exception_type == 2 {
                    removed_days.insert(offset);
                }
            }
        }

        for calendar in self.calendar.get(service_id) {
            let total_days = calendar
                .end_date
                .signed_duration_since(start_date)
                .num_days();
            for days_offset in 0..total_days + 1 {
                let current_date = start_date + Duration::days(days_offset);

                if calendar.start_date <= current_date && calendar.end_date >= current_date
                    && calendar.valid_weekday(current_date)
                    && !removed_days.contains(&days_offset)
                {
                    result.push(days_offset as u16);
                }
            }
        }

        result
    }
}

fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s == "1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_calendar() {
        let calendar = Gtfs::read_calendars(File::open("fixtures/calendar.txt").unwrap()).unwrap();
        assert_eq!(1, calendar.len());
        assert!(!calendar["service1"].monday);
        assert!(calendar["service1"].saturday);
    }

    #[test]
    fn read_calendar_dates() {
        let dates = Gtfs::read_calendar_dates(File::open("fixtures/calendar_dates.txt").unwrap()).unwrap();
        assert_eq!(2, dates.len());
        assert_eq!(2, dates["service1"].len());
        assert_eq!(2, dates["service1"][0].exception_type);
        assert_eq!(1, dates["service2"][0].exception_type);
    }

    #[test]
    fn read_stop() {
        let stops = Gtfs::read_stops(File::open("fixtures/stops.txt").unwrap()).unwrap();
        assert_eq!(5, stops.len());
        assert_eq!(LocationType::StopArea, stops[0].location_type);
        assert_eq!(LocationType::StopPoint, stops[1].location_type);
        assert_eq!(Some("1".to_owned()), stops[2].parent_station)
    }

    #[test]
    fn read_routes() {
        let routes = Gtfs::read_routes(File::open("fixtures/routes.txt").unwrap()).unwrap();
        assert_eq!(1, routes.len());
    }

    #[test]
    fn read_trips() {
        let trips = Gtfs::read_trips(File::open("fixtures/trips.txt").unwrap()).unwrap();
        assert_eq!(1, trips.len());
    }

    #[test]
    fn read_stop_times() {
        let stop_times = Gtfs::read_stop_times(File::open("fixtures/stop_times.txt").unwrap()).unwrap();
        assert_eq!(2, stop_times.len());
    }

    #[test]
    fn trip_days() {
        let gtfs = Gtfs::new("fixtures/").unwrap();
        let days = gtfs.trip_days(&"service1".to_owned(), NaiveDate::from_ymd(2017, 1, 1));
        assert_eq!(vec![6, 7, 13, 14], days);

        let days2 = gtfs.trip_days(&"service2".to_owned(), NaiveDate::from_ymd(2017, 1, 1));
        assert_eq!(vec![0], days2);
    }

    #[test]
    fn read_from_gtfs() {
        let gtfs = Gtfs::from_zip("fixtures/gtfs.zip").unwrap();
        assert_eq!(1, gtfs.calendar.len());
        assert_eq!(2, gtfs.calendar_dates.len());
        assert_eq!(5, gtfs.stops.len());
        assert_eq!(1, gtfs.routes.len());
        assert_eq!(1, gtfs.trips.len());
        assert_eq!(2, gtfs.stop_times.len());
    }

    #[test]
    fn read_from_subdirectory() {
        let gtfs = Gtfs::from_zip("fixtures/subdirectory.zip").unwrap();
        assert_eq!(1, gtfs.calendar.len());
        assert_eq!(2, gtfs.calendar_dates.len());
        assert_eq!(5, gtfs.stops.len());
        assert_eq!(1, gtfs.routes.len());
        assert_eq!(1, gtfs.trips.len());
        assert_eq!(2, gtfs.stop_times.len());
    }
}
