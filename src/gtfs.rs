use crate::{objects::*, Error, RawGtfs};
use chrono::prelude::NaiveDate;
use chrono::Duration;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Data structure with all the GTFS objects
///
/// This structure is easier to use than the [RawGtfs] structure.
#[derive(Default)]
pub struct Gtfs {
    pub read_duration: i64,
    pub calendar: HashMap<String, Calendar>,
    pub calendar_dates: HashMap<String, Vec<CalendarDate>>,
    pub stops: HashMap<String, Arc<Stop>>,
    pub routes: HashMap<String, Route>,
    pub trips: HashMap<String, Trip>,
    pub agencies: Vec<Agency>,
    pub shapes: HashMap<String, Vec<Shape>>,
    pub fare_attributes: HashMap<String, FareAttribute>,
    pub feed_info: Vec<FeedInfo>,
}

impl Gtfs {
    pub fn try_from(raw: RawGtfs) -> Result<Gtfs, Error> {
        let stops = to_stop_map(raw.stops?);
        let trips = create_trips(raw.trips?, raw.stop_times?, &stops)?;

        Ok(Gtfs {
            stops,
            routes: to_map(raw.routes?),
            trips,
            agencies: raw.agencies?,
            shapes: to_shape_map(raw.shapes.unwrap_or_else(|| Ok(Vec::new()))?),
            fare_attributes: to_map(raw.fare_attributes.unwrap_or_else(|| Ok(Vec::new()))?),
            feed_info: raw.feed_info.unwrap_or_else(|| Ok(Vec::new()))?,
            calendar: to_map(raw.calendar.unwrap_or_else(|| Ok(Vec::new()))?),
            calendar_dates: to_calendar_dates(
                raw.calendar_dates.unwrap_or_else(|| Ok(Vec::new()))?,
            ),
            read_duration: raw.read_duration,
        })
    }
}

impl Gtfs {
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

    /// Reads from an url (if starts with http), or a local path (either a directory or zipped file)
    /// To read from an url, build with read-url feature
    /// See also Gtfs::from_url and Gtfs::from_path if you don’t want the library to guess
    pub fn new(gtfs: &str) -> Result<Gtfs, Error> {
        RawGtfs::new(gtfs).and_then(Gtfs::try_from)
    }

    /// Reads the GTFS from a local zip archive or local directory
    pub fn from_path<P>(path: P) -> Result<Gtfs, Error>
    where
        P: AsRef<std::path::Path> + std::fmt::Display,
    {
        RawGtfs::from_path(path).and_then(Gtfs::try_from)
    }

    /// Reads the GTFS from a remote url
    /// The library must be built with the read-url feature
    #[cfg(feature = "read-url")]
    pub fn from_url<U: reqwest::IntoUrl>(url: U) -> Result<Gtfs, Error> {
        RawGtfs::from_url(url).and_then(Gtfs::try_from)
    }

    /// Asynchronously reads the GTFS from a remote url
    /// The library must be built with the read-url feature
    #[cfg(feature = "read-url")]
    pub async fn from_url_async<U: reqwest::IntoUrl>(url: U) -> Result<Gtfs, Error> {
        RawGtfs::from_url_async(url).await.and_then(Gtfs::try_from)
    }

    pub fn from_reader<T: std::io::Read + std::io::Seek>(reader: T) -> Result<Gtfs, Error> {
        RawGtfs::from_reader(reader).and_then(Gtfs::try_from)
    }

    pub fn trip_days(&self, service_id: &str, start_date: NaiveDate) -> Vec<u16> {
        let mut result = Vec::new();

        // Handle services given by specific days and exceptions
        let mut removed_days = HashSet::new();
        for extra_day in self
            .calendar_dates
            .get(service_id)
            .iter()
            .flat_map(|e| e.iter())
        {
            let offset = extra_day.date.signed_duration_since(start_date).num_days();
            if offset >= 0 {
                if extra_day.exception_type == Exception::Added {
                    result.push(offset as u16);
                } else if extra_day.exception_type == Exception::Deleted {
                    removed_days.insert(offset);
                }
            }
        }

        if let Some(calendar) = self.calendar.get(service_id) {
            let total_days = calendar
                .end_date
                .signed_duration_since(start_date)
                .num_days();
            for days_offset in 0..=total_days {
                let current_date = start_date + Duration::days(days_offset);

                if calendar.start_date <= current_date
                    && calendar.end_date >= current_date
                    && calendar.valid_weekday(current_date)
                    && !removed_days.contains(&days_offset)
                {
                    result.push(days_offset as u16);
                }
            }
        }

        result
    }

    pub fn get_stop<'a>(&'a self, id: &str) -> Result<&'a Stop, Error> {
        match self.stops.get(id) {
            Some(stop) => Ok(stop),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_trip<'a>(&'a self, id: &str) -> Result<&'a Trip, Error> {
        match self.trips.get(id) {
            Some(trip) => Ok(trip),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_route<'a>(&'a self, id: &str) -> Result<&'a Route, Error> {
        match self.routes.get(id) {
            Some(route) => Ok(route),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_calendar<'a>(&'a self, id: &str) -> Result<&'a Calendar, Error> {
        match self.calendar.get(id) {
            Some(calendar) => Ok(calendar),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_calendar_date<'a>(&'a self, id: &str) -> Result<&'a Vec<CalendarDate>, Error> {
        match self.calendar_dates.get(id) {
            Some(calendar_dates) => Ok(calendar_dates),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_shape<'a>(&'a self, id: &str) -> Result<&'a Vec<Shape>, Error> {
        match self.shapes.get(id) {
            Some(shape) => Ok(shape),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_fare_attributes<'a>(&'a self, id: &str) -> Result<&'a FareAttribute, Error> {
        self.fare_attributes
            .get(id)
            .ok_or_else(|| Error::ReferenceError(id.to_owned()))
    }
}

fn to_map<O: Id>(elements: impl IntoIterator<Item = O>) -> HashMap<String, O> {
    elements
        .into_iter()
        .map(|e| (e.id().to_owned(), e))
        .collect()
}

fn to_stop_map(stops: Vec<Stop>) -> HashMap<String, Arc<Stop>> {
    stops
        .into_iter()
        .map(|s| (s.id.clone(), Arc::new(s)))
        .collect()
}

fn to_shape_map(shapes: Vec<Shape>) -> HashMap<String, Vec<Shape>> {
    let mut res = HashMap::default();
    for s in shapes {
        let shape = res.entry(s.id.to_owned()).or_insert_with(Vec::new);
        shape.push(s);
    }
    res
}

fn to_calendar_dates(cd: Vec<CalendarDate>) -> HashMap<String, Vec<CalendarDate>> {
    let mut res = HashMap::default();
    for c in cd {
        let cal = res.entry(c.service_id.to_owned()).or_insert_with(Vec::new);
        cal.push(c);
    }
    res
}

fn create_trips(
    raw_trips: Vec<RawTrip>,
    raw_stop_times: Vec<RawStopTime>,
    stops: &HashMap<String, Arc<Stop>>,
) -> Result<HashMap<String, Trip>, Error> {
    let mut trips = to_map(raw_trips.into_iter().map(|rt| Trip {
        id: rt.id,
        service_id: rt.service_id,
        route_id: rt.route_id,
        stop_times: vec![],
        shape_id: rt.shape_id,
    }));
    for s in raw_stop_times {
        let trip = &mut trips
            .get_mut(&s.trip_id)
            .ok_or(Error::ReferenceError(s.trip_id.to_string()))?;
        let stop = stops
            .get(&s.stop_id)
            .ok_or(Error::ReferenceError(s.stop_id.to_string()))?;
        trip.stop_times.push(StopTime::from(&s, Arc::clone(&stop)));
    }

    for trip in &mut trips.values_mut() {
        trip.stop_times
            .sort_by(|a, b| a.stop_sequence.cmp(&b.stop_sequence));
    }
    Ok(trips)
}
