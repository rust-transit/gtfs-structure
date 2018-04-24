extern crate chrono;
extern crate csv;
#[macro_use]
extern crate derivative;
extern crate failure;
#[macro_use]
extern crate failure_derive;
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

#[derive(Fail, Debug)]
#[fail(display = "The id {} is not known", id)]
pub struct ReferenceError {
    pub id: String,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum LocationType {
    StopPoint = 0,
    StopArea = 1,
    StationEntrance = 2,
}
#[derive(Derivative)]
#[derivative(Default(bound = ""))]
#[derive(Debug, Deserialize, Copy, Clone, PartialEq)]
pub enum RouteType {
    #[serde(rename = "0")] Tramway,
    #[serde(rename = "1")] Subway,
    #[serde(rename = "2")] Rail,
    #[derivative(Default)]
    #[serde(rename = "3")]
    Bus,
    #[serde(rename = "4")] Ferry,
    #[serde(rename = "5")] CableCar,
    #[serde(rename = "6")] Gondola,
    #[serde(rename = "7")] Funicular,
}

#[derive(Debug, Deserialize)]
pub struct Calendar {
    #[serde(rename = "service_id")] pub id: String,
    #[serde(deserialize_with = "deserialize_bool")] pub monday: bool,
    #[serde(deserialize_with = "deserialize_bool")] pub tuesday: bool,
    #[serde(deserialize_with = "deserialize_bool")] pub wednesday: bool,
    #[serde(deserialize_with = "deserialize_bool")] pub thursday: bool,
    #[serde(deserialize_with = "deserialize_bool")] pub friday: bool,
    #[serde(deserialize_with = "deserialize_bool")] pub saturday: bool,
    #[serde(deserialize_with = "deserialize_bool")] pub sunday: bool,
    #[serde(deserialize_with = "deserialize_date")] pub start_date: NaiveDate,
    #[serde(deserialize_with = "deserialize_date")] pub end_date: NaiveDate,
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
    pub service_id: String,
    #[serde(deserialize_with = "deserialize_date")] pub date: NaiveDate,
    pub exception_type: u8,
}

#[derive(Debug, Deserialize)]
pub struct Stop {
    #[serde(rename = "stop_id")] pub id: String,
    pub stop_name: String,
    #[serde(deserialize_with = "deserialize_location_type", default = "default_location_type")]
    pub location_type: LocationType,
    pub parent_station: Option<String>,
    #[serde(rename = "stop_lon")] pub longitude: f32,
    #[serde(rename = "stop_lat")] pub latitude: f32,
}

#[derive(Debug, Deserialize)]
struct StopTimeGtfs {
    trip_id: String,
    #[serde(deserialize_with = "deserialize_time")] pub arrival_time: u32,
    #[serde(deserialize_with = "deserialize_time")] pub departure_time: u32,
    stop_id: String,
    stop_sequence: u16,
    pickup_type: Option<u8>,
    drop_off_type: Option<u8>,
}

#[derive(Debug)]
pub struct StopTime {
    pub arrival_time: u32,
    pub departure_time: u32,
    pub stop_id: String,
    pub pickup_type: Option<u8>,
    pub drop_off_type: Option<u8>,
    pub stop_sequence: u16,
}

impl From<StopTimeGtfs> for StopTime {
    fn from(stop_time_gtfs: StopTimeGtfs) -> Self {
        Self {
            arrival_time: stop_time_gtfs.arrival_time,
            departure_time: stop_time_gtfs.departure_time,
            stop_id: stop_time_gtfs.stop_id,
            pickup_type: stop_time_gtfs.pickup_type,
            drop_off_type: stop_time_gtfs.drop_off_type,
            stop_sequence: stop_time_gtfs.stop_sequence,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Route {
    #[serde(rename = "route_id")] pub id: String,
    #[serde(rename = "route_short_name")] pub short_name: String,
    #[serde(rename = "route_long_name")] pub long_name: String,
    pub route_type: RouteType,
}

#[derive(Debug, Deserialize)]
pub struct Trip {
    #[serde(rename = "trip_id")] pub id: String,
    pub service_id: String,
    pub route_id: String,
    #[serde(skip)]
    pub stop_times: Vec<StopTime>,
}

fn deserialize_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s, "%Y%m%d").map_err(serde::de::Error::custom)
}

pub fn parse_time(s: String) -> Result<u32, Error> {
    let v: Vec<&str> = s.split(':').collect();
    Ok(&v[0].parse()? * 3600u32 + &v[1].parse()? * 60u32 + &v[2].parse()?)
}

fn deserialize_time<'de, D>(deserializer: D) -> Result<u32, D::Error>
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
    pub stops: HashMap<String, Stop>,
    pub routes: HashMap<String, Route>,
    pub trips: HashMap<String, Trip>,
}

impl Gtfs {
    pub fn print_stats(&self) {
        println!("GTFS data:");
        println!("  Read in {} ms", self.read_duration);
        println!("  Stops: {}", self.stops.len());
        println!("  Routes: {}", self.routes.len());
        println!("  Trips: {}", self.trips.len());
    }

    fn empty() -> Gtfs {
        Gtfs {
            read_duration: 0,
            calendar: HashMap::new(),
            calendar_dates: HashMap::new(),
            stops: HashMap::new(),
            routes: HashMap::new(),
            trips: HashMap::new(),
        }
    }

    pub fn new(path: &str) -> Result<Gtfs, Error> {
        let now = Utc::now();
        let p = Path::new(path);
        let calendar_file = File::open(p.join("calendar.txt"))?;
        let stops_file = File::open(p.join("stops.txt"))?;
        let calendar_dates_file = File::open(p.join("calendar_dates.txt"))?;
        let routes_file = File::open(p.join("routes.txt"))?;
        let mut trips = Gtfs::read_trips(File::open(p.join("trips.txt"))?)?;
        let stop_times_file = File::open(p.join("stop_times.txt"))?;
        Gtfs::read_stop_times(stop_times_file, &mut trips)?;

        Ok(Gtfs {
            calendar: Gtfs::read_calendars(calendar_file)?,
            stops: Gtfs::read_stops(stops_file)?,
            calendar_dates: Gtfs::read_calendar_dates(calendar_dates_file)?,
            routes: Gtfs::read_routes(routes_file)?,
            trips: trips,
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
        let mut stop_times_index = None;
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
            } else if file.name().ends_with("trips.txt") {
                result.trips = Gtfs::read_trips(file)?;
            } else if file.name().ends_with("stop_times.txt") {
                stop_times_index = Some(i);
            }
        }
        Gtfs::read_stop_times(archive.by_index(stop_times_index.unwrap())?, &mut result.trips)?;

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

    fn read_calendar_dates<T: std::io::Read>(
        reader: T,
    ) -> Result<HashMap<String, Vec<CalendarDate>>, Error> {
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

    fn read_stops<T: std::io::Read>(reader: T) -> Result<HashMap<String, Stop>, Error> {
        let mut reader = csv::Reader::from_reader(reader);
        Ok(reader
            .deserialize()
            .map(|res| res.map(|e: Stop| (e.id.to_owned(), e)))
            .collect::<Result<_, _>>()?)
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
        let trips = reader
            .deserialize()
            .map(|res| res.map(|e: Trip| (e.id.to_owned(), e)))
            .collect::<Result<_, _>>()?;

        Ok(trips)
    }

    fn read_stop_times<T: std::io::Read>(reader: T, trips: &mut HashMap<String, Trip>) -> Result<(), Error> {
        for stop_time in csv::Reader::from_reader(reader).deserialize() {
            let s: StopTimeGtfs = stop_time?;
            let mut trip = trips.get_mut(&s.trip_id).ok_or(ReferenceError{id: s.trip_id.to_string()})?;
            trip.stop_times.push(StopTime::from(s));
        }

        for (_, trip) in trips {
            trip.stop_times.sort_by(|a, b| a.stop_sequence.cmp(&b.stop_sequence))
        }

        Ok(())
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

    pub fn get_stop<'a>(&'a self, id: &str) -> Result<&'a Stop, ReferenceError> {
        match self.stops.get(id) {
            Some(stop) => Ok(stop),
            None => Err(ReferenceError { id: id.to_owned() }),
        }
    }

    pub fn get_trip<'a>(&'a self, id: &str) -> Result<&'a Trip, ReferenceError> {
        match self.trips.get(id) {
            Some(trip) => Ok(trip),
            None => Err(ReferenceError { id: id.to_owned() }),
        }
    }

    pub fn get_route<'a>(&'a self, id: &str) -> Result<&'a Route, ReferenceError> {
        match self.routes.get(id) {
            Some(route) => Ok(route),
            None => Err(ReferenceError { id: id.to_owned() }),
        }
    }

    pub fn get_calendar<'a>(&'a self, id: &str) -> Result<&'a Calendar, ReferenceError> {
        match self.calendar.get(id) {
            Some(calendar) => Ok(calendar),
            None => Err(ReferenceError { id: id.to_owned() }),
        }
    }

    pub fn get_calendar_date<'a>(
        &'a self,
        id: &str,
    ) -> Result<&'a Vec<CalendarDate>, ReferenceError> {
        match self.calendar_dates.get(id) {
            Some(calendar_dates) => Ok(calendar_dates),
            None => Err(ReferenceError { id: id.to_owned() }),
        }
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
        let dates =
            Gtfs::read_calendar_dates(File::open("fixtures/calendar_dates.txt").unwrap()).unwrap();
        assert_eq!(2, dates.len());
        assert_eq!(2, dates["service1"].len());
        assert_eq!(2, dates["service1"][0].exception_type);
        assert_eq!(1, dates["service2"][0].exception_type);
    }

    #[test]
    fn read_stop() {
        let stops = Gtfs::read_stops(File::open("fixtures/stops.txt").unwrap()).unwrap();
        assert_eq!(5, stops.len());
        assert_eq!(
            LocationType::StopArea,
            stops.get("stop1").unwrap().location_type
        );
        assert_eq!(
            LocationType::StopPoint,
            stops.get("stop2").unwrap().location_type
        );
        assert_eq!(
            Some("1".to_owned()),
            stops.get("stop3").unwrap().parent_station
        );
    }

    #[test]
    fn read_routes() {
        let routes = Gtfs::read_routes(File::open("fixtures/routes.txt").unwrap()).unwrap();
        assert_eq!(1, routes.len());
        assert_eq!(RouteType::Bus, routes.get("1").unwrap().route_type);
    }

    #[test]
    fn read_trips() {
        let trips = Gtfs::read_trips(File::open("fixtures/trips.txt").unwrap()).unwrap();
        assert_eq!(1, trips.len());
    }

    #[test]
    fn read_stop_times() {
        let stop_times =
            Gtfs::read_stop_times(File::open("fixtures/stop_times.txt").unwrap()).unwrap();
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

        assert!(gtfs.get_calendar("service1").is_ok());
        assert!(gtfs.get_calendar_date("service1").is_ok());
        assert!(gtfs.get_stop("stop1").is_ok());
        assert!(gtfs.get_route("1").is_ok());
        assert!(gtfs.get_trip("trip1").is_ok());

        assert_eq!("Utopia", gtfs.get_stop("Utopia").unwrap_err().id);
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
