use chrono::{Datelike, NaiveDate, Weekday};
use rgb::RGB8;
use serde::de::{self, Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use std::fmt;
use std::sync::Arc;

/// Objects that have an identifier implement this trait
///
/// Those identifier are technical and should not be shown to travellers
pub trait Id {
    /// Identifier of the object
    fn id(&self) -> &str;
}

/// Trait to introspect what is the object’s type (stop, route…)
pub trait Type {
    /// What is the type of the object
    fn object_type(&self) -> ObjectType;
}

/// All the objects type from the GTFS specification that this library reads
#[derive(Debug, Serialize, Eq, PartialEq, Hash)]
pub enum ObjectType {
    /// [Agency] <https://gtfs.org/reference/static/#agencytxt>
    Agency,
    /// [Stop] <https://gtfs.org/reference/static/#stopstxt>
    Stop,
    /// [Route] <https://gtfs.org/reference/static/#routestxt>
    Route,
    /// [Trip] <https://gtfs.org/reference/static/#tripstxt>
    Trip,
    /// [Calendar] <https://gtfs.org/reference/static/#calendartxt>
    Calendar,
    /// [Shape] <https://gtfs.org/reference/static/#shapestxt>
    Shape,
    /// [FareAttribute] <https://gtfs.org/reference/static/#fare_rulestxt>
    Fare,
}

/// Describes the kind of [Stop]. See <https://gtfs.org/reference/static/#stopstxt> `location_type`
#[derive(Derivative, Debug, Copy, Clone, PartialEq, Serialize)]
#[derivative(Default(bound = ""))]
pub enum LocationType {
    /// Stop (or Platform). A location where passengers board or disembark from a transit vehicle. Is called a platform when defined within a parent_station
    #[derivative(Default)]
    StopPoint = 0,
    /// Station. A physical structure or area that contains one or more platform
    StopArea = 1,
    /// A location where passengers can enter or exit a station from the street. If an entrance/exit belongs to multiple stations, it can be linked by pathways to both, but the data provider must pick one of them as parent
    StationEntrance = 2,
    /// A location within a station, not matching any other [Stop::location_type], which can be used to link together pathways define in pathways.txt.
    GenericNode = 3,
    ///A specific location on a platform, where passengers can board and/or alight vehicles
    BoardingArea = 4,
}

impl<'de> Deserialize<'de> for LocationType {
    fn deserialize<D>(deserializer: D) -> Result<LocationType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "1" => LocationType::StopArea,
            "2" => LocationType::StationEntrance,
            "3" => LocationType::GenericNode,
            "4" => LocationType::BoardingArea,
            _ => LocationType::StopPoint,
        })
    }
}

/// Describes the kind of [Route]. See <https://gtfs.org/reference/static/#routestxt> `route_type`
///
/// -ome route types are extended GTFS (<https://developers.google.com/transit/gtfs/reference/extended-route-types)>
#[derive(Debug, Derivative, Copy, Clone, PartialEq, Eq, Hash)]
#[derivative(Default(bound = ""))]
pub enum RouteType {
    /// Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area
    Tramway,
    /// Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area
    Subway,
    /// Used for intercity or long-distance travel
    Rail,
    /// Used for short- and long-distance bus routes
    #[derivative(Default)]
    Bus,
    /// Used for short- and long-distance boat service
    Ferry,
    /// Used for street-level rail cars where the cable runs beneath the vehicle, e.g., cable car in San Francisco
    CableCar,
    /// Aerial lift, suspended cable car (e.g., gondola lift, aerial tramway). Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables
    Gondola,
    /// Any rail system designed for steep inclines
    Funicular,
    /// (extended) Used for intercity bus services
    Coach,
    /// (extended) Airplanes
    Air,
    /// (extended) Taxi, Cab
    Taxi,
    /// (extended) any other value
    Other(u16),
}

impl<'de> Deserialize<'de> for RouteType {
    fn deserialize<D>(deserializer: D) -> Result<RouteType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let i = u16::deserialize(deserializer)?;

        let hundreds = i / 100;
        Ok(match (i, hundreds) {
            (0, _) | (_, 9) => RouteType::Tramway,
            (1, _) | (_, 4) => RouteType::Subway,
            (2, _) | (_, 1) => RouteType::Rail,
            (3, _) | (_, 7) | (_, 8) => RouteType::Bus,
            (4, _) | (_, 10) | (_, 12) => RouteType::Ferry,
            (5, _) => RouteType::CableCar,
            (6, _) | (_, 13) => RouteType::Gondola,
            (7, _) | (_, 14) => RouteType::Funicular,
            (_, 2) => RouteType::Coach,
            (_, 11) => RouteType::Air,
            (_, 15) => RouteType::Taxi,
            _ => RouteType::Other(i),
        })
    }
}

impl Serialize for RouteType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Note: for extended route type, we might loose the initial precise route type
        serializer.serialize_u16(match self {
            RouteType::Tramway => 0,
            RouteType::Subway => 1,
            RouteType::Rail => 2,
            RouteType::Bus => 3,
            RouteType::Ferry => 4,
            RouteType::CableCar => 5,
            RouteType::Gondola => 6,
            RouteType::Funicular => 7,
            RouteType::Coach => 200,
            RouteType::Air => 1100,
            RouteType::Taxi => 1500,
            RouteType::Other(i) => *i,
        })
    }
}

/// Describes if and how a traveller can board or alight the vehicle. See <https://gtfs.org/reference/static/#stop_timestxt> `pickup_type` and `dropoff_type`
#[derive(Debug, Derivative, Serialize, Copy, Clone, PartialEq)]
#[derivative(Default(bound = ""))]
pub enum PickupDropOffType {
    /// Regularly scheduled pickup or drop off (default when empty).
    #[derivative(Default)]
    Regular = 0,
    /// No pickup or drop off available.
    NotAvailable = 1,
    /// Must phone agency to arrange pickup or drop off.
    ArrangeByPhone = 2,
    /// Must coordinate with driver to arrange pickup or drop off.
    CoordinateWithDriver = 3,
}

impl<'de> Deserialize<'de> for PickupDropOffType {
    fn deserialize<D>(deserializer: D) -> Result<PickupDropOffType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "0" => PickupDropOffType::Regular,
            "1" => PickupDropOffType::NotAvailable,
            "2" => PickupDropOffType::ArrangeByPhone,
            "3" => PickupDropOffType::CoordinateWithDriver,
            _ => PickupDropOffType::Regular,
        })
    }
}

/// Indicates whether a rider can board the transit vehicle anywhere along the vehicle’s travel path
///
/// Those values are only defined on <https://developers.google.com/transit/gtfs/reference#routestxt,> not on <https://gtfs.org/reference/static/#routestxt>
#[derive(Debug, Derivative, Serialize, Copy, Clone, PartialEq)]
#[derivative(Default(bound = ""))]
pub enum ContinuousPickupDropOff {
    /// Continuous stopping pickup or drop off.
    Continuous = 0,
    /// No continuous stopping pickup or drop off (default when empty).
    #[derivative(Default)]
    NotAvailable = 1,
    /// Must phone agency to arrange continuous stopping pickup or drop off.
    ArrangeByPhone = 2,
    /// Must coordinate with driver to arrange continuous stopping pickup or drop off.
    CoordinateWithDriver = 3,
}

impl<'de> Deserialize<'de> for ContinuousPickupDropOff {
    fn deserialize<D>(deserializer: D) -> Result<ContinuousPickupDropOff, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "0" => ContinuousPickupDropOff::Continuous,
            "1" => ContinuousPickupDropOff::NotAvailable,
            "2" => ContinuousPickupDropOff::ArrangeByPhone,
            "3" => ContinuousPickupDropOff::CoordinateWithDriver,
            _ => ContinuousPickupDropOff::NotAvailable,
        })
    }
}

/// Describes if the stop time is exact or not. See <https://gtfs.org/reference/static/#stop_timestxt> `timepoint`
#[derive(Debug, Derivative, Serialize, Copy, Clone, PartialEq)]
#[derivative(Default)]
pub enum TimepointType {
    /// Times are considered approximate
    #[serde(rename = "0")]
    Approximate = 0,
    /// Times are considered exact
    #[derivative(Default)]
    Exact = 1,
}

impl<'de> Deserialize<'de> for TimepointType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        match s.as_str() {
            "" | "1" => Ok(Self::Exact),
            "0" => Ok(Self::Approximate),
            v => Err(serde::de::Error::custom(format!(
                "invalid value for timepoint: {}",
                v
            ))),
        }
    }
}

/// A calender describes on which days the vehicle runs. See <https://gtfs.org/reference/static/#calendartxt>
#[derive(Debug, Deserialize, Serialize)]
pub struct Calendar {
    /// Unique technical identifier (not for the traveller) of this calendar
    #[serde(rename = "service_id")]
    pub id: String,
    /// Does the service run on mondays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub monday: bool,
    /// Does the service run on tuesdays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub tuesday: bool,
    /// Does the service run on wednesdays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub wednesday: bool,
    /// Does the service run on thursdays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub thursday: bool,
    /// Does the service run on fridays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub friday: bool,
    /// Does the service run on saturdays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub saturday: bool,
    /// Does the service run on sundays
    #[serde(
        deserialize_with = "deserialize_bool",
        serialize_with = "serialize_bool"
    )]
    pub sunday: bool,
    /// Start service day for the service interval
    #[serde(
        deserialize_with = "deserialize_date",
        serialize_with = "serialize_date"
    )]
    pub start_date: NaiveDate,
    /// End service day for the service interval. This service day is included in the interval
    #[serde(
        deserialize_with = "deserialize_date",
        serialize_with = "serialize_date"
    )]
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

/// Generic enum to define if a service (like wheelchair boarding) is available
#[derive(Serialize, Deserialize, Debug, Derivative, PartialEq, Eq, Hash, Clone, Copy)]
#[derivative(Default)]
pub enum Availability {
    /// No information if the service is available
    #[derivative(Default)]
    #[serde(rename = "0")]
    InformationNotAvailable,
    /// The service is available
    #[serde(rename = "1")]
    Available,
    /// The service is not available
    #[serde(rename = "2")]
    NotAvailable,
}

impl Calendar {
    /// Returns true if there is a service running on that day
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

/// Defines if a [CalendarDate] is added or deleted from a [Calendar]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Exception {
    /// There will be a service on that day
    #[serde(rename = "1")]
    Added,
    /// There won’t be a service on that day
    #[serde(rename = "2")]
    Deleted,
}

/// Defines a specific date that can be added or removed from a [Calendar]. See <https://gtfs.org/reference/static/#calendar_datestxt>
#[derive(Debug, Deserialize, Serialize)]
pub struct CalendarDate {
    /// Identifier of the service that is modified at this date
    pub service_id: String,
    #[serde(
        deserialize_with = "deserialize_date",
        serialize_with = "serialize_date"
    )]
    /// Date where the service will be added or deleted
    pub date: NaiveDate,
    /// Is the service added or deleted
    pub exception_type: Exception,
}

/// A physical stop, station or area. See <https://gtfs.org/reference/static/#stopstxt>
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Stop {
    /// Unique technical identifier (not for the traveller) of the stop
    #[serde(rename = "stop_id")]
    pub id: String,
    /// Short text or a number that identifies the location for riders
    #[serde(rename = "stop_code")]
    pub code: Option<String>,
    ///Name of the location. Use a name that people will understand in the local and tourist vernacular
    #[serde(rename = "stop_name")]
    pub name: String,
    /// Description of the location that provides useful, quality information
    #[serde(default, rename = "stop_desc")]
    pub description: String,
    /// Type of the location
    #[serde(default)]
    pub location_type: LocationType,
    /// Defines hierarchy between the different locations
    pub parent_station: Option<String>,
    /// Identifies the fare zone for a stop
    pub zone_id: Option<String>,
    /// URL of a web page about the location
    #[serde(rename = "stop_url")]
    pub url: Option<String>,
    /// Longitude of the stop
    #[serde(deserialize_with = "de_with_optional_float")]
    #[serde(rename = "stop_lon", default)]
    pub longitude: Option<f64>,
    /// Latitude of the stop
    #[serde(deserialize_with = "de_with_optional_float")]
    #[serde(rename = "stop_lat", default)]
    pub latitude: Option<f64>,
    /// Timezone of the location
    #[serde(rename = "stop_timezone")]
    pub timezone: Option<String>,
    /// Indicates whether wheelchair boardings are possible from the location
    #[serde(deserialize_with = "de_with_empty_default", default)]
    pub wheelchair_boarding: Availability,
    /// Level of the location. The same level can be used by multiple unlinked stations
    pub level_id: Option<String>,
    /// Platform identifier for a platform stop (a stop belonging to a station)
    pub platform_code: Option<String>,
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

/// A [StopTime] where the relations with [Trip] and [Stop] have not been tested
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RawStopTime {
    /// [Trip] to which this stop time belongs to
    pub trip_id: String,
    /// Arrival time of the stop time.
    /// It's an option since the intermediate stops can have have no arrival
    /// and this arrival needs to be interpolated
    #[serde(
        deserialize_with = "deserialize_optional_time",
        serialize_with = "serialize_optional_time"
    )]
    pub arrival_time: Option<u32>,
    /// Departure time of the stop time.
    /// It's an option since the intermediate stops can have have no departure
    /// and this departure needs to be interpolated
    #[serde(
        deserialize_with = "deserialize_optional_time",
        serialize_with = "serialize_optional_time"
    )]
    pub departure_time: Option<u32>,
    /// Identifier of the [Stop] where the vehicle stops
    pub stop_id: String,
    /// Order of stops for a particular trip. The values must increase along the trip but do not need to be consecutive
    pub stop_sequence: u16,
    /// Text that appears on signage identifying the trip's destination to riders
    pub stop_headsign: Option<String>,
    /// Indicates pickup method
    #[serde(default)]
    pub pickup_type: PickupDropOffType,
    /// Indicates drop off method
    #[serde(default)]
    pub drop_off_type: PickupDropOffType,
    /// Indicates whether a rider can board the transit vehicle anywhere along the vehicle’s travel path
    #[serde(default)]
    pub continuous_pickup: ContinuousPickupDropOff,
    /// Indicates whether a rider can alight from the transit vehicle at any point along the vehicle’s travel path
    #[serde(default)]
    pub continuous_drop_off: ContinuousPickupDropOff,
    /// Actual distance traveled along the associated shape, from the first stop to the stop specified in this record. This field specifies how much of the shape to draw between any two stops during a trip
    pub shape_dist_traveled: Option<f32>,
    /// Indicates if arrival and departure times for a stop are strictly adhered to by the vehicle or if they are instead approximate and/or interpolated times
    #[serde(default)]
    pub timepoint: TimepointType,
}

/// The moment where a vehicle, running on [Trip] stops at a [Stop]. See <https://gtfs.org/reference/static/#stopstxt>
#[derive(Debug, Default)]
pub struct StopTime {
    /// Arrival time of the stop time.
    /// It's an option since the intermediate stops can have have no arrival
    /// and this arrival needs to be interpolated
    pub arrival_time: Option<u32>,
    /// [Stop] where the vehicle stops
    pub stop: Arc<Stop>,
    /// Departure time of the stop time.
    /// It's an option since the intermediate stops can have have no departure
    /// and this departure needs to be interpolated
    pub departure_time: Option<u32>,
    /// Indicates pickup method
    pub pickup_type: PickupDropOffType,
    /// Indicates drop off method
    pub drop_off_type: PickupDropOffType,
    /// Order of stops for a particular trip. The values must increase along the trip but do not need to be consecutive
    pub stop_sequence: u16,
    /// Text that appears on signage identifying the trip's destination to riders
    pub stop_headsign: Option<String>,
    /// Indicates whether a rider can board the transit vehicle anywhere along the vehicle’s travel path
    pub continuous_pickup: ContinuousPickupDropOff,
    /// Indicates whether a rider can alight from the transit vehicle at any point along the vehicle’s travel path
    pub continuous_drop_off: ContinuousPickupDropOff,
    /// Actual distance traveled along the associated shape, from the first stop to the stop specified in this record. This field specifies how much of the shape to draw between any two stops during a trip
    pub shape_dist_traveled: Option<f32>,
    /// Indicates if arrival and departure times for a stop are strictly adhered to by the vehicle or if they are instead approximate and/or interpolated times
    pub timepoint: TimepointType,
}

impl StopTime {
    /// Creates [StopTime] by linking a [RawStopTime::stop_id] to the actual [Stop]
    pub fn from(stop_time_gtfs: &RawStopTime, stop: Arc<Stop>) -> Self {
        Self {
            arrival_time: stop_time_gtfs.arrival_time,
            departure_time: stop_time_gtfs.departure_time,
            stop,
            pickup_type: stop_time_gtfs.pickup_type,
            drop_off_type: stop_time_gtfs.drop_off_type,
            stop_sequence: stop_time_gtfs.stop_sequence,
            stop_headsign: stop_time_gtfs.stop_headsign.clone(),
            continuous_pickup: stop_time_gtfs.continuous_pickup,
            continuous_drop_off: stop_time_gtfs.continuous_drop_off,
            shape_dist_traveled: stop_time_gtfs.shape_dist_traveled,
            timepoint: stop_time_gtfs.timepoint,
        }
    }
}

/// A route is a commercial line (there can be various stop sequences for a same line). See <https://gtfs.org/reference/static/#routestxt>
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Route {
    /// Unique technical (not for the traveller) identifier for the route
    #[serde(rename = "route_id")]
    pub id: String,
    /// Short name of a route. This will often be a short, abstract identifier like "32", "100X", or "Green" that riders use to identify a route, but which doesn't give any indication of what places the route serves
    #[serde(rename = "route_short_name")]
    pub short_name: String,
    /// Full name of a route. This name is generally more descriptive than the [Route::short_name]] and often includes the route's destination or stop
    #[serde(rename = "route_long_name")]
    pub long_name: String,
    /// Description of a route that provides useful, quality information
    #[serde(rename = "route_desc")]
    pub desc: Option<String>,
    /// Indicates the type of transportation used on a route
    pub route_type: RouteType,
    /// URL of a web page about the particular route
    #[serde(rename = "route_url")]
    pub url: Option<String>,
    /// Agency for the specified route
    pub agency_id: Option<String>,
    #[serde(rename = "route_sort_order")]
    /// Orders the routes in a way which is ideal for presentation to customers. Routes with smaller route_sort_order values should be displayed first.
    pub route_order: Option<u32>,
    /// Route color designation that matches public facing material
    #[serde(
        deserialize_with = "de_with_optional_color",
        serialize_with = "serialize_optional_color",
        default
    )]
    pub route_color: Option<RGB8>,
    /// Legible color to use for text drawn against a background of [Route::route_color]
    #[serde(
        deserialize_with = "de_with_optional_color",
        serialize_with = "serialize_optional_color",
        default
    )]
    pub route_text_color: Option<RGB8>,
    /// Indicates whether a rider can board the transit vehicle anywhere along the vehicle’s travel path
    #[serde(default)]
    pub continuous_pickup: ContinuousPickupDropOff,
    /// Indicates whether a rider can alight from the transit vehicle at any point along the vehicle’s travel path
    #[serde(default)]
    pub continuous_drop_off: ContinuousPickupDropOff,
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

/// Defines the direction of a [Trip], only for display, not for routing. See <https://gtfs.org/reference/static/#tripstxt> `direction_id`
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub enum DirectionType {
    /// Travel in one direction (e.g. outbound travel).
    #[serde(rename = "0")]
    Outbound,
    /// Travel in the opposite direction (e.g. inbound travel).
    #[serde(rename = "1")]
    Inbound,
}

/// Is the [Trip] wheelchair accessible. See <https://gtfs.org/reference/static/#tripstxt> `wheelchair_boarding`
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub enum WheelChairAccessibleType {
    /// No accessibility information for the trip
    #[serde(rename = "0")]
    NoAccessibilityInfo,
    /// Vehicle being used on this particular trip can accommodate at least one rider in a wheelchair
    #[serde(rename = "1")]
    AtLeastOneWheelChair,
    /// No riders in wheelchairs can be accommodated on this trip
    #[serde(rename = "2")]
    NotWheelChairAccessible,
}

/// Is the [Trip] accessible with a bike. See <https://gtfs.org/reference/static/#tripstxt> `bikes_allowed`
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub enum BikesAllowedType {
    /// No bike information for the trip
    #[serde(rename = "0")]
    NoBikeInfo,
    /// Vehicle being used on this particular trip can accommodate at least one bicycle
    #[serde(rename = "1")]
    AtLeastOneBike,
    /// No bicycles are allowed on this trip
    #[serde(rename = "2")]
    NoBikesAllowed,
}

/// A [Trip] where the relationships with other objects have not been checked
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RawTrip {
    /// Unique technical (not for the traveller) identifier for the Trip
    #[serde(rename = "trip_id")]
    pub id: String,
    /// References the [Calendar] on which this trip runs
    pub service_id: String,
    /// References along which [Route] this trip runs
    pub route_id: String,
    /// Shape of the trip
    pub shape_id: Option<String>,
    /// Text that appears on signage identifying the trip's destination to riders
    pub trip_headsign: Option<String>,
    /// Public facing text used to identify the trip to riders, for instance, to identify train numbers for commuter rail trips
    pub trip_short_name: Option<String>,
    /// Indicates the direction of travel for a trip. This field is not used in routing; it provides a way to separate trips by direction when publishing time tables
    pub direction_id: Option<DirectionType>,
    /// Identifies the block to which the trip belongs. A block consists of a single trip or many sequential trips made using the same vehicle, defined by shared service days and block_id. A block_id can have trips with different service days, making distinct blocks
    pub block_id: Option<String>,
    /// Indicates wheelchair accessibility
    pub wheelchair_accessible: Option<WheelChairAccessibleType>,
    /// Indicates whether bikes are allowed
    pub bikes_allowed: Option<BikesAllowedType>,
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

/// A Trip is a vehicle that follows a sequence of [StopTime] on certain days. See <https://gtfs.org/reference/static/#tripstxt>
#[derive(Debug, Default)]
pub struct Trip {
    /// Unique technical identifier (not for the traveller) for the Trip
    pub id: String,
    /// References the [Calendar] on which this trip runs
    pub service_id: String,
    /// References along which [Route] this trip runs
    pub route_id: String,
    /// All the [StopTime] that define the trip
    pub stop_times: Vec<StopTime>,
    /// Text that appears on signage identifying the trip's destination to riders
    pub shape_id: Option<String>,
    /// Text that appears on signage identifying the trip's destination to riders
    pub trip_headsign: Option<String>,
    /// Public facing text used to identify the trip to riders, for instance, to identify train numbers for commuter rail trips
    pub trip_short_name: Option<String>,
    /// Indicates the direction of travel for a trip. This field is not used in routing; it provides a way to separate trips by direction when publishing time tables
    pub direction_id: Option<DirectionType>,
    /// Identifies the block to which the trip belongs. A block consists of a single trip or many sequential trips made using the same vehicle, defined by shared service days and block_id. A block_id can have trips with different service days, making distinct blocks
    pub block_id: Option<String>,
    /// Indicates wheelchair accessibility
    pub wheelchair_accessible: Option<WheelChairAccessibleType>,
    /// Indicates whether bikes are allowed
    pub bikes_allowed: Option<BikesAllowedType>,
    /// During which periods the trip runs by frequency and not by fixed timetable
    pub frequencies: Vec<Frequency>,
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

/// General informations about the agency running the network. See <https://gtfs.org/reference/static/#agencytxt>
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Agency {
    /// Unique technical (not for the traveller) identifier for the Agency
    #[serde(rename = "agency_id")]
    pub id: Option<String>,
    ///Full name of the transit agency
    #[serde(rename = "agency_name")]
    pub name: String,
    /// Full name of the transit agency.
    #[serde(rename = "agency_url")]
    pub url: String,
    /// Timezone where the transit agency is located
    #[serde(rename = "agency_timezone")]
    pub timezone: String,
    /// Primary language used by this transit agency
    #[serde(rename = "agency_lang")]
    pub lang: Option<String>,
    /// A voice telephone number for the specified agency
    #[serde(rename = "agency_phone")]
    pub phone: Option<String>,
    /// URL of a web page that allows a rider to purchase tickets or other fare instruments for that agency online
    #[serde(rename = "agency_fare_url")]
    pub fare_url: Option<String>,
    /// Email address actively monitored by the agency’s customer service department
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

/// A single geographical point decribing the shape of a [Trip]. See <https://gtfs.org/reference/static/#shapestxt>
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Shape {
    /// Unique technical (not for the traveller) identifier for the Shape
    #[serde(rename = "shape_id")]
    pub id: String,
    #[serde(rename = "shape_pt_lat", default)]
    /// Latitude of a shape point
    pub latitude: f64,
    /// Longitude of a shape point
    #[serde(rename = "shape_pt_lon", default)]
    pub longitude: f64,
    /// Sequence in which the shape points connect to form the shape. Values increase along the trip but do not need to be consecutive.
    #[serde(rename = "shape_pt_sequence")]
    pub sequence: usize,
    /// Actual distance traveled along the shape from the first shape point to the point specified in this record. Used by trip planners to show the correct portion of the shape on a map
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

/// Defines one possible fare. See <https://gtfs.org/reference/static/#fare_attributestxt>
#[derive(Debug, Serialize, Deserialize)]
pub struct FareAttribute {
    /// Unique technical (not for the traveller) identifier for the FareAttribute
    #[serde(rename = "fare_id")]
    pub id: String,
    /// Fare price, in the unit specified by [FareAttribute::currency]
    pub price: String,
    /// Currency used to pay the fare.
    #[serde(rename = "currency_type")]
    pub currency: String,
    ///Indicates when the fare must be paid
    pub payment_method: PaymentMethod,
    /// Indicates the number of transfers permitted on this fare
    pub transfers: Transfers,
    /// Identifies the relevant agency for a fare
    pub agency_id: Option<String>,
    /// Length of time in seconds before a transfer expires
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

/// Defines where a [FareAttribute] can be paid
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub enum PaymentMethod {
    /// Fare is paid on board
    #[serde(rename = "0")]
    Aboard,
    /// Fare must be paid before boarding
    #[serde(rename = "1")]
    PreBoarding,
}

/// A [Frequency] before being merged into the corresponding [Trip]
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RawFrequency {
    /// References the [Trip] that uses frequency
    pub trip_id: String,
    /// Time at which the first vehicle departs from the first stop of the trip
    #[serde(
        deserialize_with = "deserialize_time",
        serialize_with = "serialize_time"
    )]
    pub start_time: u32,
    /// Time at which service changes to a different headway (or ceases) at the first stop in the trip
    #[serde(
        deserialize_with = "deserialize_time",
        serialize_with = "serialize_time"
    )]
    pub end_time: u32,
    /// Time, in seconds, between departures from the same stop (headway) for the trip, during the time interval specified by start_time and end_time
    pub headway_secs: u32,
    /// Indicates the type of service for a trip
    pub exact_times: Option<ExactTimes>,
}

/// Defines if the [Frequency] is exact (the vehicle runs exactly every n minutes) or not
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq)]
pub enum ExactTimes {
    /// Frequency-based trips
    #[serde(rename = "0")]
    FrequencyBased,
    /// Schedule-based trips with the exact same headway throughout the day.
    #[serde(rename = "1")]
    ScheduleBased,
}

/// Timetables can be defined by the frequency of their vehicles. See <<https://gtfs.org/reference/static/#frequenciestxt>>
#[derive(Debug, Default)]
pub struct Frequency {
    /// Time at which the first vehicle departs from the first stop of the trip
    pub start_time: u32,
    /// Time at which service changes to a different headway (or ceases) at the first stop in the trip
    pub end_time: u32,
    /// Time, in seconds, between departures from the same stop (headway) for the trip, during the time interval specified by start_time and end_time
    pub headway_secs: u32,
    /// Indicates the type of service for a trip
    pub exact_times: Option<ExactTimes>,
}

impl Frequency {
    /// Converts from a [RawFrequency] to a [Frequency]
    pub fn from(frequency: &RawFrequency) -> Self {
        Self {
            start_time: frequency.start_time,
            end_time: frequency.end_time,
            headway_secs: frequency.headway_secs,
            exact_times: frequency.exact_times,
        }
    }
}

/// Defines how many transfers can be done with on [FareAttribute]
#[derive(Debug, Derivative, Copy, Clone, PartialEq)]
#[derivative(Default(bound = ""))]
pub enum Transfers {
    /// Unlimited transfers are permitted
    #[derivative(Default)]
    Unlimited,
    /// No transfers permitted on this fare
    NoTransfer,
    /// Riders may transfer once
    UniqueTransfer,
    ///Riders may transfer twice
    TwoTransfers,
    /// Other transfer values
    Other(u16),
}

impl<'de> Deserialize<'de> for Transfers {
    fn deserialize<D>(deserializer: D) -> Result<Transfers, D::Error>
    where
        D: Deserializer<'de>,
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

impl Serialize for Transfers {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Transfers::NoTransfer => serializer.serialize_u16(0),
            Transfers::UniqueTransfer => serializer.serialize_u16(1),
            Transfers::TwoTransfers => serializer.serialize_u16(2),
            Transfers::Other(a) => serializer.serialize_u16(*a),
            Transfers::Unlimited => serializer.serialize_none(),
        }
    }
}

/// Meta-data about the feed. See <https://gtfs.org/reference/static/#feed_infotxt>
#[derive(Debug, Serialize, Deserialize)]
pub struct FeedInfo {
    /// Full name of the organization that publishes the dataset.
    #[serde(rename = "feed_publisher_name")]
    pub name: String,
    /// URL of the dataset publishing organization's website
    #[serde(rename = "feed_publisher_url")]
    pub url: String,
    /// Default language used for the text in this dataset
    #[serde(rename = "feed_lang")]
    pub lang: String,
    /// Defines the language that should be used when the data consumer doesn’t know the language of the rider
    pub default_lang: Option<String>,
    /// The dataset provides complete and reliable schedule information for service in the period from this date
    #[serde(
        deserialize_with = "deserialize_option_date",
        serialize_with = "serialize_option_date",
        rename = "feed_start_date",
        default
    )]
    pub start_date: Option<NaiveDate>,
    ///The dataset provides complete and reliable schedule information for service in the period until this date
    #[serde(
        deserialize_with = "deserialize_option_date",
        serialize_with = "serialize_option_date",
        rename = "feed_end_date",
        default
    )]
    pub end_date: Option<NaiveDate>,
    /// String that indicates the current version of their GTFS dataset
    #[serde(rename = "feed_version")]
    pub version: Option<String>,
    /// Email address for communication regarding the GTFS dataset and data publishing practices
    #[serde(rename = "feed_contact_email")]
    pub contact_email: Option<String>,
    /// URL for contact information, a web-form, support desk, or other tools for communication regarding the GTFS dataset and data publishing practices
    #[serde(rename = "feed_contact_url")]
    pub contact_url: Option<String>,
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

fn serialize_date<S>(date: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(format!("{}{}{}", date.year(), date.month(), date.day()).as_str())
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

fn serialize_option_date<S>(date: &Option<NaiveDate>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match date {
        None => serializer.serialize_none(),
        Some(d) => {
            serializer.serialize_str(format!("{}{}{}", d.year(), d.month(), d.day()).as_str())
        }
    }
}

fn parse_time_impl(v: Vec<&str>) -> Result<u32, std::num::ParseIntError> {
    let hours = &v[0].parse()?;
    let minutes = &v[1].parse()?;
    let seconds = &v[2].parse()?;
    Ok(hours * 3600u32 + minutes * 60u32 + seconds)
}

fn parse_time(s: &str) -> Result<u32, crate::Error> {
    let v: Vec<&str> = s.trim_start().split(':').collect();
    if v.len() != 3 {
        Err(crate::Error::InvalidTime(s.to_owned()))
    } else {
        Ok(parse_time_impl(v).map_err(|_| crate::Error::InvalidTime(s.to_owned()))?)
    }
}

fn deserialize_time<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_time(&s).map_err(de::Error::custom)
}

fn serialize_time<S>(time: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(format!("{}", time).as_str())
}

fn deserialize_optional_time<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<String>::deserialize(deserializer)?;

    match s {
        None => Ok(None),
        Some(t) => Ok(Some(parse_time(&t).map_err(de::Error::custom)?)),
    }
}

fn serialize_optional_time<S>(time: &Option<u32>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match time {
        None => serializer.serialize_none(),
        Some(t) => serializer.serialize_str(format!("{}", t).as_str()),
    }
}

fn de_with_optional_float<'de, D>(de: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(de).and_then(|s| {
        let s = s.trim();
        if s.is_empty() {
            Ok(None)
        } else {
            s.parse().map(Some).map_err(de::Error::custom)
        }
    })
}

fn parse_color(s: &str) -> Result<RGB8, crate::Error> {
    if s.len() != 6 {
        return Err(crate::Error::InvalidColor(s.to_owned()));
    }
    let r =
        u8::from_str_radix(&s[0..2], 16).map_err(|_| crate::Error::InvalidColor(s.to_owned()))?;
    let g =
        u8::from_str_radix(&s[2..4], 16).map_err(|_| crate::Error::InvalidColor(s.to_owned()))?;
    let b =
        u8::from_str_radix(&s[4..6], 16).map_err(|_| crate::Error::InvalidColor(s.to_owned()))?;
    Ok(RGB8::new(r, g, b))
}

fn de_with_optional_color<'de, D>(de: D) -> Result<Option<RGB8>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(de).and_then(|s| {
        let s = s.trim();
        if s.is_empty() {
            Ok(None)
        } else {
            parse_color(s).map(Some).map_err(de::Error::custom)
        }
    })
}

fn serialize_optional_color<S>(color: &Option<RGB8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match color {
        None => serializer.serialize_none(),
        Some(RGB8 { r, g, b }) => {
            serializer.serialize_str(format!("{:02X}{:02X}{:02X}", r, g, b).as_str())
        }
    }
}

fn de_with_empty_default<'de, T: Default, D>(de: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(de).map(|opt| opt.unwrap_or_else(Default::default))
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

fn serialize_bool<S>(value: &bool, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if *value {
        serializer.serialize_u8(1)
    } else {
        serializer.serialize_u8(0)
    }
}
