use chrono::{Datelike, NaiveDate, Weekday};
use failure::Error;
use serde::de::{self, Deserialize, Deserializer};
use std::fmt;
use std::sync::Arc;

pub trait Id {
    fn id(&self) -> &str;
}

pub trait Type {
    fn object_type(&self) -> ObjectType;
}

#[derive(Debug, Serialize, Eq, PartialEq, Hash)]
pub enum ObjectType {
    Agency,
    Stop,
    Route,
    Trip,
    Calendar,
    Shape,
    Fare,
}

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

impl Default for LocationType {
    fn default() -> LocationType {
        LocationType::StopPoint
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum RouteType {
    Tramway,
    Subway,
    Rail,
    Bus,
    Ferry,
    CableCar,
    Gondola,
    Funicular,
    // Any other value than 0..7 is invalid in the GTFS
    // However, some bad files might have other values
    // We don’t want to stop nor skip too soon during deserialization
    Other(u16),
}

impl Default for RouteType {
    fn default() -> RouteType {
        RouteType::Bus
    }
}

impl<'de> ::serde::Deserialize<'de> for RouteType {
    fn deserialize<D>(deserializer: D) -> Result<RouteType, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        let i = u16::deserialize(deserializer)?;
        Ok(match i {
            0 => RouteType::Tramway,
            1 => RouteType::Subway,
            2 => RouteType::Rail,
            3 => RouteType::Bus,
            4 => RouteType::Ferry,
            5 => RouteType::CableCar,
            6 => RouteType::Gondola,
            7 => RouteType::Funicular,
            _ => RouteType::Other(i),
        })
    }
}

#[derive(Derivative)]
#[derivative(Default(bound = ""))]
#[derive(Debug, Deserialize, Copy, Clone, PartialEq)]
pub enum PickupDropOffType {
    #[derivative(Default)]
    #[serde(rename = "0")]
    Regular,
    #[serde(rename = "1")]
    NotAvailable,
    #[serde(rename = "2")]
    ArrangeByPhone,
    #[serde(rename = "3")]
    CoordinateWithDriver,
}

#[derive(Debug, Deserialize)]
pub struct Calendar {
    #[serde(rename = "service_id")]
    pub id: String,
    #[serde(deserialize_with = "deserialize_bool")]
    pub monday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    pub tuesday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    pub wednesday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    pub thursday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    pub friday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    pub saturday: bool,
    #[serde(deserialize_with = "deserialize_bool")]
    pub sunday: bool,
    #[serde(deserialize_with = "deserialize_date")]
    pub start_date: NaiveDate,
    #[serde(deserialize_with = "deserialize_date")]
    pub end_date: NaiveDate,
}

impl Type for Calendar {
    fn object_type(&self) -> ObjectType {
        ObjectType::Calendar
    }
}

impl Id for Calendar {
    fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for Calendar {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}—{}", self.start_date, self.end_date)
    }
}

#[derive(Serialize, Deserialize, Debug, Derivative, PartialEq, Eq, Hash, Clone, Copy)]
#[derivative(Default)]
pub enum Availability {
    #[derivative(Default)]
    #[serde(rename = "0")]
    InformationNotAvailable,
    #[serde(rename = "1")]
    Available,
    #[serde(rename = "2")]
    NotAvailable,
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Exception {
    #[serde(rename = "1")]
    Added,
    #[serde(rename = "2")]
    Deleted,
}

#[derive(Debug, Deserialize)]
pub struct CalendarDate {
    pub service_id: String,
    #[serde(deserialize_with = "deserialize_date")]
    pub date: NaiveDate,
    pub exception_type: Exception,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct Stop {
    #[serde(rename = "stop_id")]
    pub id: String,
    #[serde(rename = "stop_code")]
    pub code: Option<String>,
    #[serde(rename = "stop_name")]
    pub name: String,
    #[serde(default, rename = "stop_desc")]
    pub description: String,
    #[serde(
        deserialize_with = "deserialize_location_type",
        default = "default_location_type"
    )]
    pub location_type: LocationType,
    pub parent_station: Option<String>,
    #[serde(deserialize_with = "de_with_trimed_float")]
    #[serde(rename = "stop_lon", default)]
    pub longitude: f64,
    #[serde(deserialize_with = "de_with_trimed_float")]
    #[serde(rename = "stop_lat", default)]
    pub latitude: f64,
    #[serde(rename = "stop_timezone")]
    pub timezone: Option<String>,
    #[serde(deserialize_with = "de_with_empty_default", default)]
    pub wheelchair_boarding: Availability,
}

impl Type for Stop {
    fn object_type(&self) -> ObjectType {
        ObjectType::Stop
    }
}

impl Id for Stop {
    fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct RawStopTime {
    pub trip_id: String,
    #[serde(deserialize_with = "deserialize_time")]
    pub arrival_time: u32,
    #[serde(deserialize_with = "deserialize_time")]
    pub departure_time: u32,
    pub stop_id: String,
    pub stop_sequence: u16,
    pub pickup_type: Option<PickupDropOffType>,
    pub drop_off_type: Option<PickupDropOffType>,
}

#[derive(Debug, Default)]
pub struct StopTime {
    pub arrival_time: u32,
    pub stop: Arc<Stop>,
    pub departure_time: u32,
    pub pickup_type: Option<PickupDropOffType>,
    pub drop_off_type: Option<PickupDropOffType>,
    pub stop_sequence: u16,
}

impl StopTime {
    pub fn from(stop_time_gtfs: &RawStopTime, stop: Arc<Stop>) -> Self {
        Self {
            arrival_time: stop_time_gtfs.arrival_time,
            departure_time: stop_time_gtfs.departure_time,
            stop,
            pickup_type: stop_time_gtfs.pickup_type,
            drop_off_type: stop_time_gtfs.drop_off_type,
            stop_sequence: stop_time_gtfs.stop_sequence,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct Route {
    #[serde(rename = "route_id")]
    pub id: String,
    #[serde(rename = "route_short_name")]
    pub short_name: String,
    #[serde(rename = "route_long_name")]
    pub long_name: String,
    pub route_type: RouteType,
    pub agency_id: Option<String>,
    pub route_order: Option<u32>,
}

impl Type for Route {
    fn object_type(&self) -> ObjectType {
        ObjectType::Route
    }
}

impl Id for Route {
    fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for Route {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if !self.long_name.is_empty() {
            write!(f, "{}", self.long_name)
        } else {
            write!(f, "{}", self.short_name)
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct RawTrip {
    #[serde(rename = "trip_id")]
    pub id: String,
    pub service_id: String,
    pub route_id: String,
}

impl Type for RawTrip {
    fn object_type(&self) -> ObjectType {
        ObjectType::Trip
    }
}

impl Id for RawTrip {
    fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for RawTrip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "route id: {}, service id: {}",
            self.route_id, self.service_id
        )
    }
}

#[derive(Debug, Default)]
pub struct Trip {
    pub id: String,
    pub service_id: String,
    pub route_id: String,
    pub stop_times: Vec<StopTime>,
}

impl Type for Trip {
    fn object_type(&self) -> ObjectType {
        ObjectType::Trip
    }
}

impl Id for Trip {
    fn id(&self) -> &str {
        &self.id
    }
}

impl fmt::Display for Trip {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "route id: {}, service id: {}",
            self.route_id, self.service_id
        )
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct Agency {
    #[serde(rename = "agency_id")]
    pub id: Option<String>,
    #[serde(rename = "agency_name")]
    pub name: String,
    #[serde(rename = "agency_url")]
    pub url: String,
    #[serde(rename = "agency_timezone")]
    pub timezone: String,
    #[serde(rename = "agency_lang")]
    pub lang: Option<String>,
    #[serde(rename = "agency_phone")]
    pub phone: Option<String>,
    #[serde(rename = "agency_fare_url")]
    pub fare_url: Option<String>,
    #[serde(rename = "agency_email")]
    pub email: Option<String>,
}

impl Type for Agency {
    fn object_type(&self) -> ObjectType {
        ObjectType::Agency
    }
}

impl Id for Agency {
    fn id(&self) -> &str {
        match &self.id {
            None => "",
            Some(id) => id,
        }
    }
}

impl fmt::Display for Agency {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct Shape {
    #[serde(rename = "shape_id")]
    pub id: String,
    #[serde(rename = "shape_pt_lat", default)]
    pub latitude: f64,
    #[serde(rename = "shape_pt_lon", default)]
    pub longitude: f64,
    #[serde(rename = "shape_pt_sequence")]
    pub sequence: usize,
    #[serde(rename = "shape_dist_traveled")]
    pub dist_traveled: Option<f32>,
}

impl Type for Shape {
    fn object_type(&self) -> ObjectType {
        ObjectType::Shape
    }
}

impl Id for Shape {
    fn id(&self) -> &str {
        &self.id
    }
}

#[derive(Debug, Deserialize)]
pub struct FareAttribute {
    #[serde(rename = "fare_id")]
    pub id: String,
    pub price: String,
    #[serde(rename = "currency_type")]
    pub currency: String,
    pub payment_method: PaymentMethod,
    pub transfers: Transfers,
    pub agency_id: Option<String>,
    pub transfer_duration: Option<usize>,
}

impl Id for FareAttribute {
    fn id(&self) -> &str {
        &self.id
    }
}

impl Type for FareAttribute {
    fn object_type(&self) -> ObjectType {
        ObjectType::Fare
    }
}

#[derive(Debug, Deserialize, Copy, Clone, PartialEq)]
pub enum PaymentMethod {
    #[serde(rename = "0")]
    Aboard,
    #[serde(rename = "1")]
    PreBoarding,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Transfers {
    Unlimited,
    NoTransfer,
    UniqueTransfer,
    TwoTransfers,
    Other(u16),
}

impl<'de> ::serde::Deserialize<'de> for Transfers {
    fn deserialize<D>(deserializer: D) -> Result<Transfers, D::Error>
    where
        D: ::serde::Deserializer<'de>,
    {
        let i = Option::<u16>::deserialize(deserializer)?;
        Ok(match i {
            Some(0) => Transfers::NoTransfer,
            Some(1) => Transfers::UniqueTransfer,
            Some(2) => Transfers::TwoTransfers,
            Some(a) => Transfers::Other(a),
            None => Transfers::default(),
        })
    }
}

impl Default for Transfers {
    fn default() -> Transfers {
        Transfers::Unlimited
    }
}

#[derive(Debug, Deserialize)]
pub struct FeedInfo {
    #[serde(rename = "feed_publisher_name")]
    pub name: String,
    #[serde(rename = "feed_publisher_url")]
    pub url: String,
    #[serde(rename = "feed_lang")]
    pub lang: String,
    #[serde(
        deserialize_with = "deserialize_option_date",
        rename = "feed_start_date"
    )]
    pub start_date: Option<NaiveDate>,
    #[serde(deserialize_with = "deserialize_option_date", rename = "feed_end_date")]
    pub end_date: Option<NaiveDate>,
    #[serde(rename = "feed_version")]
    pub version: Option<String>,
}

impl fmt::Display for FeedInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

fn deserialize_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s, "%Y%m%d").map_err(serde::de::Error::custom)
}

fn deserialize_option_date<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?
        .map(|s| NaiveDate::parse_from_str(&s, "%Y%m%d").map_err(serde::de::Error::custom));
    match s {
        Some(Ok(s)) => Ok(Some(s)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

pub fn parse_time(s: &str) -> Result<u32, Error> {
    let v: Vec<&str> = s.split(':').collect();
    Ok(&v[0].parse()? * 3600u32 + &v[1].parse()? * 60u32 + &v[2].parse()?)
}

fn deserialize_time<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = String::deserialize(deserializer)?;
    parse_time(&s).map_err(de::Error::custom)
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

fn de_with_trimed_float<'de, D>(de: D) -> Result<f64, D::Error>
where
    D: ::serde::Deserializer<'de>,
{
    String::deserialize(de).and_then(|s| s.trim().parse().map_err(de::Error::custom))
}

pub fn de_with_empty_default<'de, T: Default, D>(de: D) -> Result<T, D::Error>
where
    D: ::serde::Deserializer<'de>,
    T: ::serde::Deserialize<'de>,
{
    use serde::Deserialize;
    Option::<T>::deserialize(de).map(|opt| opt.unwrap_or_else(Default::default))
}

fn default_location_type() -> LocationType {
    LocationType::StopPoint
}

fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match &*s {
        "0" => Ok(false),
        "1" => Ok(true),
        &_ => Err(serde::de::Error::custom(format!(
            "Invalid value `{}`, expected 0 or 1",
            s
        ))),
    }
}
