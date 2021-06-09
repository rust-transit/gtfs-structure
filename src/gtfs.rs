use crate::{objects::*, Error, RawGtfs};
use chrono::prelude::NaiveDate;
use chrono::Duration;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
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
    pub translations_by_id: HashMap<TranslationByIdKey, String>,
    pub translations_by_value: HashMap<TranslationByValueKey, String>,
}

impl TryFrom<RawGtfs> for Gtfs {
    type Error = Error;
    fn try_from(raw: RawGtfs) -> Result<Gtfs, Error> {
        let stops = to_stop_map(raw.stops?);
        let trips = create_trips(raw.trips?, raw.stop_times?, &stops)?;
        let (translations_by_id, translations_by_value) = create_translations(
            raw.translations.unwrap_or(Ok(vec!()))?
        )?;

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
            translations_by_id,
            translations_by_value,
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

    pub fn translate(
        &self,
        table_name: &str,
        field_name: &str,
        language: &str,
        record_id: &str,
        record_sub_id: Option<&str>,
        field_value: &String
    ) -> String {
        if let Some(ret) = self.translations_by_id.get(&TranslationByIdKey{
            table_name: table_name.to_string(),
            field_name: field_name.to_string(),
            language: language.to_string(),
            record_id: record_id.to_string(),
            record_sub_id: record_sub_id.map(|x| x.to_string()),
        }) {
            return ret.to_string();
        }

        if let Some(ret) = self.translations_by_value.get(&TranslationByValueKey{
            table_name: table_name.to_string(),
            field_name: field_name.to_string(),
            language: language.to_string(),
            field_value: field_value.to_string(),
        }) {
            return ret.to_string();
        }

        field_value.to_string()
    }

    pub fn get_stop<'a>(&'a self, id: &str) -> Result<&'a Stop, Error> {
        match self.stops.get(id) {
            Some(stop) => Ok(stop),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_stop_translated(
        &self,
        id: &str,
        language: &str
    ) -> Result<Stop, Error> {
        let stop = self.get_stop(id)?;
        Ok(stop.to_owned().translate(self, language))
    }

    pub fn get_trip<'a>(&'a self, id: &str) -> Result<&'a Trip, Error> {
        match self.trips.get(id) {
            Some(trip) => Ok(trip),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_trip_translated(
        &self,
        id: &str,
        language: &str
    ) -> Result<Trip, Error> {
        let trip = self.get_trip(id)?;
        Ok(trip.to_owned().translate(self, language))
    }

    pub fn get_route<'a>(&'a self, id: &str) -> Result<&'a Route, Error> {
        match self.routes.get(id) {
            Some(route) => Ok(route),
            None => Err(Error::ReferenceError(id.to_owned())),
        }
    }

    pub fn get_route_translated(
        &self,
        id: &str,
        language: &str
    ) -> Result<Route, Error> {
        let route = self.get_route(id)?;
        Ok(route.to_owned().translate(self, language))
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
    // we sort the shape by it's pt_sequence
    for (_key, shapes) in &mut res {
        shapes.sort_by_key(|s| s.sequence);
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
        trip_headsign: rt.trip_headsign,
        trip_short_name: rt.trip_short_name,
        direction_id: rt.direction_id,
        block_id: rt.block_id,
        wheelchair_accessible: rt.wheelchair_accessible,
        bikes_allowed: rt.bikes_allowed,
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

fn create_translations(
    raw_translations: Vec<Translation>
) -> Result<(
    HashMap<TranslationByIdKey, String>,
    HashMap<TranslationByValueKey, String>
), Error> {
    let mut translations_by_id = HashMap::new();
    let mut translations_by_value = HashMap::new();

    for translation in raw_translations {
        if translation.record_id.is_some() {
            // Make sure it is not forbidden
            if translation.field_value.is_some() ||
                translation.table_name == "feed_info".to_string() {
                return Err(Error::InvalidTranslation(
                        "record_id was defined when it was forbidden".to_string()
                ));
            }

            // Make sure record_sub_id is there if and only if it is required
            if translation.table_name == "stop_times".to_string() &&
                translation.record_sub_id.is_none() {
                return Err(Error::InvalidTranslation(
                        "record_sub_id was not set when it was required".to_string()
                ));
            }

            translations_by_id.insert(TranslationByIdKey {
                table_name: translation.table_name,
                field_name: translation.field_name,
                language: translation.language,
                record_id: translation.record_id.unwrap(),
                record_sub_id: translation.record_sub_id,
            }, translation.translation);
        } else if translation.field_value.is_some() {
            // Make sure it is not forbidden
            if translation.record_id.is_some() ||
                translation.record_sub_id.is_some() ||
                translation.table_name == "feed_info".to_string() {
                return Err(Error::InvalidTranslation(
                        "field_value was defined when it was forbidden".to_string()
                ));
            }

            translations_by_value.insert(TranslationByValueKey {
                table_name: translation.table_name,
                field_name: translation.field_name,
                language: translation.language,
                field_value: translation.field_value.unwrap(),
            }, translation.translation);
        }
    }

    return Ok((translations_by_id, translations_by_value));
}
