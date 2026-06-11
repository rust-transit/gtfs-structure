use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};

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
    /// [Pathway] <https://gtfs.org/schedule/reference/#pathwaystxt>
    Pathway,
}

/// Describes the kind of [Stop]. See <https://gtfs.org/reference/static/#stopstxt> `location_type`
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum LocationType {
    /// Stop (or Platform). A location where passengers board or disembark from a transit vehicle. Is called a platform when defined within a parent_station
    #[default]
    StopPoint,
    /// Station. A physical structure or area that contains one or more platform
    StopArea,
    /// A location where passengers can enter or exit a station from the street. If an entrance/exit belongs to multiple stations, it can be linked by pathways to both, but the data provider must pick one of them as parent
    StationEntrance,
    /// A location within a station, not matching any other [Stop::location_type], which can be used to link together pathways define in pathways.txt.
    GenericNode,
    /// A specific location on a platform, where passengers can board and/or alight vehicles
    BoardingArea,
    /// An unknown value
    Unknown(i16),
}

fn serialize_i16_as_str<S: Serializer>(s: S, value: i16) -> Result<S::Ok, S::Error> {
    s.serialize_str(&value.to_string())
}
impl<'de> Deserialize<'de> for LocationType {
    fn deserialize<D>(deserializer: D) -> Result<LocationType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Ok(match s {
            "" | "0" => LocationType::StopPoint,
            "1" => LocationType::StopArea,
            "2" => LocationType::StationEntrance,
            "3" => LocationType::GenericNode,
            "4" => LocationType::BoardingArea,
            s => LocationType::Unknown(s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid value for LocationType, must be an integer: {s}"
                ))
            })?),
        })
    }
}

impl Serialize for LocationType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Note: for extended route type, we might loose the initial precise route type
        serialize_i16_as_str(
            serializer,
            match self {
                LocationType::StopPoint => 0,
                LocationType::StopArea => 1,
                LocationType::StationEntrance => 2,
                LocationType::GenericNode => 3,
                LocationType::BoardingArea => 4,
                LocationType::Unknown(i) => *i,
            },
        )
    }
}

#[derive(Debug, Clone, derive_more::TryFrom, Copy, PartialEq, Eq, Hash)]
#[repr(i16)]
#[try_from(repr)]
/// Google Transit extended route types [https://developers.google.com/transit/gtfs/reference/extended-route-types](https://developers.google.com/transit/gtfs/reference/extended-route-types)
pub enum ExtendedRouteType {
    /// Railway Service.
    Railway = 100,
    /// High Speed Rail Service. Examples: TGV (FR), ICE (DE), Eurostar (GB).
    HighSpeedRail = 101,
    /// Long Distance Trains. Examples: InterCity/EuroCity.
    LongDistanceTrains = 102,
    /// Inter Regional Rail Service. Examples: InterRegio (DE), Cross County Rail (GB).
    InterRegionalRail = 103,
    /// Car Transport Rail Service.
    CarTransportRail = 104,
    /// Sleeper Rail Service. Example: GNER Sleeper (GB).
    SleeperRail = 105,
    /// Regional Rail Service. Examples: TER (FR), Regionalzug (DE).
    RegionalRail = 106,
    /// Tourist Railway Service. Example: Romney, Hythe & Dymchurch (GB).
    TouristRailway = 107,
    /// Rail Shuttle (Within Complex). Examples: Gatwick Shuttle (GB), Sky Line (DE).
    RailShuttleWithinComplex = 108,
    /// Suburban Railway. Examples: S-Bahn (DE), RER (FR), S-tog (Kopenhagen).
    SuburbanRailway = 109,
    /// Replacement Rail Service.
    ReplacementRail = 110,
    /// Special Rail Service.
    SpecialRail = 111,
    /// Lorry Transport Rail Service.
    LorryTransportRail = 112,
    /// All Rail Services.
    OtherRail = 113,
    /// Cross-Country Rail Service.
    CrossCountryRail = 114,
    /// Vehicle Transport Rail Service.
    VehicleTransportRail = 115,
    /// Rack and Pinion Railway. Examples: Rochers de Naye (CH), Dolderbahn (CH).
    RackAndPinionRailway = 116,
    /// Additional Rail Service.
    AdditionalRail = 117,

    /// Coach Service.
    Coach = 200,
    /// International Coach Service. Examples: EuroLine, Touring.
    InternationalCoach = 201,
    /// National Coach Service. Example: National Express (GB).
    NationalCoach = 202,
    /// Shuttle Coach Service. Examples: Roissy Bus (FR), Reading-Heathrow (GB).
    ShuttleCoach = 203,
    /// Regional Coach Service.
    RegionalCoach = 204,
    /// Special Coach Service.
    SpecialCoach = 205,
    /// Sightseeing Coach Service.
    SightseeingCoach = 206,
    /// Tourist Coach Service.
    TouristCoach = 207,
    /// Commuter Coach Service.
    CommuterCoach = 208,
    /// All Coach Services.
    OtherCoach = 209,

    /// Urban Railway Service.
    UrbanRailway = 400,
    /// Metro Service. Example: Métro de Paris.
    Metro = 401,
    /// Underground Service. Examples: London Underground, U-Bahn.
    Underground = 402,
    /// Urban Railway Service.
    UrbanRailwayServiceDetail = 403,
    /// All Urban Railway Services.
    OtherUrbanRailway = 404,
    /// Monorail.
    Monorail = 405,

    /// Bus Service.
    Bus = 700,
    /// Regional Bus Service. Example: Eastbourne-Maidstone (GB).
    RegionalBus = 701,
    /// Express Bus Service. Example: X19 Wokingham-Heathrow (GB).
    ExpressBus = 702,
    /// Stopping Bus Service. Example: 38 London: Clapton Pond-Victoria (GB).
    StoppingBus = 703,
    /// Local Bus Service.
    LocalBus = 704,
    /// Night Bus Service. Example: N prefixed buses in London (GB).
    NightBus = 705,
    /// Post Bus Service. Example: Maidstone P4 (GB).
    PostBus = 706,
    /// Special Needs Bus.
    SpecialNeedsBus = 707,
    /// Mobility Bus Service.
    MobilityBus = 708,
    /// Mobility Bus for Registered Disabled.
    MobilityBusForRegisteredDisabled = 709,
    /// Sightseeing Bus.
    SightseeingBus = 710,
    /// Shuttle Bus. Example: 747 Heathrow-Gatwick Airport Service (GB).
    ShuttleBus = 711,
    /// School Bus.
    SchoolBus = 712,
    /// School and Public Service Bus.
    SchoolAndPublicServiceBus = 713,
    /// Rail Replacement Bus Service.
    RailReplacementBus = 714,
    /// Demand and Response Bus Service.
    DemandAndResponseBus = 715,
    /// All Bus Services.
    OtherBus = 716,

    /// Trolleybus Service.
    Trolleybus = 800,

    /// Tram Service.
    Tram = 900,
    /// City Tram Service.
    CityTram = 901,
    /// Local Tram Service. Examples: Munich (DE), Brussels (BE), Croydon (GB).
    LocalTram = 902,
    /// Regional Tram Service.
    RegionalTram = 903,
    /// Sightseeing Tram Service. Example: Blackpool Seafront (GB).
    SightseeingTram = 904,
    /// Shuttle Tram Service.
    ShuttleTram = 905,
    /// All Tram Services.
    OtherTram = 906,

    /// Water Transport Service.
    WaterTransport = 1000,
    /// Air Service.
    Air = 1100,
    /// Ferry Service.
    Ferry = 1200,

    /// Aerial Lift Service. Examples: Telefèric de Montjuïc (ES), Saleve (CH), Roosevelt Island Tramway (US).
    AerialLift = 1300,
    /// Telecabin Service.
    Telecabin = 1301,
    /// Cable Car Service.
    CableCar = 1302,
    /// Elevator Service.
    Elevator = 1303,
    /// Chair Lift Service.
    ChairLift = 1304,
    /// Drag Lift Service.
    DragLift = 1305,
    /// Small Telecabin Service.
    SmallTelecabin = 1306,
    /// All Telecabin Services.
    OtherTelecabin = 1307,

    // Funicular Service
    /// Funicular Service. Example: Rigiblick (Zürich, CH).
    Funicular = 1400,

    /// Taxi Service.
    Taxi = 1500,
    /// Communal Taxi Service. Examples: Marshrutka (RU), dolmuş (TR).
    CommunalTaxi = 1501,
    /// Water Taxi Service.
    WaterTaxi = 1502,
    /// Rail Taxi Service.
    RailTaxi = 1503,
    /// Bike Taxi Service.
    BikeTaxi = 1504,
    /// Licensed Taxi Service.
    LicensedTaxi = 1505,
    /// Private Hire Service Vehicle.
    PrivateHireServiceVehicle = 1506,
    /// All Taxi Services.
    OtherTaxi = 1507,

    /// Miscellaneous Service.
    MiscellaneousService = 1700,
    /// Horse-drawn Carriage.
    HorseDrawnCarriage = 1702,
}

/// Describes the kind of [Route]. See <https://gtfs.org/reference/static/#routestxt> `route_type`
///
/// Some route types are extended GTFS (<https://developers.google.com/transit/gtfs/reference/extended-route-types)>
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum RouteType {
    /// Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area
    Tramway,
    /// Subway, Metro. Any underground rail system within a metropolitan area.
    Subway,
    /// Used for intercity or long-distance travel
    Rail,
    /// Used for short- and long-distance bus routes
    #[default]
    Bus,
    /// Used for short- and long-distance boat service
    Ferry,
    /// Used for street-level rail cars where the cable runs beneath the vehicle, e.g., cable car in San Francisco
    CableCar,
    /// Aerial lift, suspended cable car (e.g., gondola lift, aerial tramway). Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables
    Gondola,
    /// Any rail system designed for steep inclines
    Funicular,
    /// Google Transit extended route types <https://developers.google.com/transit/gtfs/reference/extended-route-types>
    Extended(ExtendedRouteType),
    /// Any other value
    Other(i16),
}

impl ExtendedRouteType {
    /// Convert extended route types to standard route types if possible. All route types that can not be represented in standard route types at all are kept as-is.
    /// This loses precision.
    pub fn to_standard_route_type(&self) -> RouteType {
        let int = *self as i16;
        let hundreds = int / 100;
        match hundreds {
            1 => RouteType::Rail,
            4 => RouteType::Subway,
            2 | 7 | 8 => RouteType::Bus,
            9 => RouteType::Tramway,
            10 | 12 => RouteType::Ferry,
            13 => RouteType::Gondola,
            14 => RouteType::Funicular,
            _ => RouteType::Extended(*self),
        }
    }
}

impl<'de> Deserialize<'de> for RouteType {
    fn deserialize<D>(deserializer: D) -> Result<RouteType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let i = i16::deserialize(deserializer)?;
        if let Ok(ert) = ExtendedRouteType::try_from(i) {
            return Ok(RouteType::Extended(ert));
        }

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

impl Serialize for RouteType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i16(match self {
            RouteType::Tramway => 0,
            RouteType::Subway => 1,
            RouteType::Rail => 2,
            RouteType::Bus => 3,
            RouteType::Ferry => 4,
            RouteType::CableCar => 5,
            RouteType::Gondola => 6,
            RouteType::Funicular => 7,
            RouteType::Extended(ert) => *ert as i16,
            RouteType::Other(i) => *i,
        })
    }
}

/// Describes if and how a traveller can board or alight the vehicle. See <https://gtfs.org/reference/static/#stop_timestxt> `pickup_type` and `dropoff_type`
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PickupDropOffType {
    /// Regularly scheduled pickup or drop off (default when empty).
    #[default]
    Regular,
    /// No pickup or drop off available.
    NotAvailable,
    /// Must phone agency to arrange pickup or drop off.
    ArrangeByPhone,
    /// Must coordinate with driver to arrange pickup or drop off.
    CoordinateWithDriver,
    /// An unknown value not in the specification
    Unknown(i16),
}

impl<'de> Deserialize<'de> for PickupDropOffType {
    fn deserialize<D>(deserializer: D) -> Result<PickupDropOffType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Ok(match s {
            "" | "0" => PickupDropOffType::Regular,
            "1" => PickupDropOffType::NotAvailable,
            "2" => PickupDropOffType::ArrangeByPhone,
            "3" => PickupDropOffType::CoordinateWithDriver,
            s => PickupDropOffType::Unknown(s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid value for PickupDropOffType, must be an integer: {s}"
                ))
            })?),
        })
    }
}

impl Serialize for PickupDropOffType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Note: for extended route type, we might loose the initial precise route type
        serialize_i16_as_str(
            serializer,
            match self {
                PickupDropOffType::Regular => 0,
                PickupDropOffType::NotAvailable => 1,
                PickupDropOffType::ArrangeByPhone => 2,
                PickupDropOffType::CoordinateWithDriver => 3,
                PickupDropOffType::Unknown(i) => *i,
            },
        )
    }
}

/// Indicates whether a rider can board the transit vehicle anywhere along the vehicle’s travel path
///
/// Those values are only defined on <https://developers.google.com/transit/gtfs/reference#routestxt,> not on <https://gtfs.org/reference/static/#routestxt>
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ContinuousPickupDropOff {
    /// Continuous stopping pickup or drop off.
    Continuous,
    /// No continuous stopping pickup or drop off (default when empty).
    #[default]
    NotAvailable,
    /// Must phone agency to arrange continuous stopping pickup or drop off.
    ArrangeByPhone,
    /// Must coordinate with driver to arrange continuous stopping pickup or drop off.
    CoordinateWithDriver,
    /// An unknown value not in the specification
    Unknown(i16),
}

impl Serialize for ContinuousPickupDropOff {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Note: for extended route type, we might loose the initial precise route type
        serialize_i16_as_str(
            serializer,
            match self {
                ContinuousPickupDropOff::Continuous => 0,
                ContinuousPickupDropOff::NotAvailable => 1,
                ContinuousPickupDropOff::ArrangeByPhone => 2,
                ContinuousPickupDropOff::CoordinateWithDriver => 3,
                ContinuousPickupDropOff::Unknown(i) => *i,
            },
        )
    }
}

impl<'de> Deserialize<'de> for ContinuousPickupDropOff {
    fn deserialize<D>(deserializer: D) -> Result<ContinuousPickupDropOff, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Ok(match s {
            "0" => ContinuousPickupDropOff::Continuous,
            "" | "1" => ContinuousPickupDropOff::NotAvailable,
            "2" => ContinuousPickupDropOff::ArrangeByPhone,
            "3" => ContinuousPickupDropOff::CoordinateWithDriver,
            s => ContinuousPickupDropOff::Unknown(s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid value for ContinuousPickupDropOff, must be an integer: {s}"
                ))
            })?),
        })
    }
}

/// Describes if the stop time is exact or not. See <https://gtfs.org/reference/static/#stop_timestxt> `timepoint`
#[derive(Debug, Default, Serialize, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TimepointType {
    /// Times are considered approximate
    #[serde(rename = "0")]
    Approximate = 0,
    /// Times are considered exact
    #[default]
    #[serde(rename = "1")]
    Exact = 1,
}

impl<'de> Deserialize<'de> for TimepointType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        match s {
            "" | "1" => Ok(Self::Exact),
            "0" => Ok(Self::Approximate),
            v => Err(serde::de::Error::custom(format!(
                "invalid value for timepoint: {v}"
            ))),
        }
    }
}

/// Generic enum to define if a service (like wheelchair boarding) is available
#[derive(Debug, Default, PartialEq, Eq, Hash, Clone, Copy)]
pub enum Availability {
    /// No information if the service is available
    #[default]
    InformationNotAvailable,
    /// The service is available
    Available,
    /// The service is not available
    NotAvailable,
    /// An unknown value not in the specification
    Unknown(i16),
}

impl<'de> Deserialize<'de> for Availability {
    fn deserialize<D>(deserializer: D) -> Result<Availability, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Ok(match s {
            "" | "0" => Availability::InformationNotAvailable,
            "1" => Availability::Available,
            "2" => Availability::NotAvailable,
            s => Availability::Unknown(s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid value for Availability, must be an integer: {s}"
                ))
            })?),
        })
    }
}

impl Serialize for Availability {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Note: for extended route type, we might loose the initial precise route type
        serialize_i16_as_str(
            serializer,
            match self {
                Availability::InformationNotAvailable => 0,
                Availability::Available => 1,
                Availability::NotAvailable => 2,
                Availability::Unknown(i) => *i,
            },
        )
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

/// Defines the direction of a [Trip], only for display, not for routing. See <https://gtfs.org/reference/static/#tripstxt> `direction_id`
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq, Hash)]
pub enum DirectionType {
    /// Travel in one direction (e.g. outbound travel).
    #[serde(rename = "0")]
    Outbound,
    /// Travel in the opposite direction (e.g. inbound travel).
    #[serde(rename = "1")]
    Inbound,
}

/// Is the [Trip] accessible with a bike. See <https://gtfs.org/reference/static/#tripstxt> `bikes_allowed`
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum BikesAllowedType {
    /// No bike information for the trip
    #[default]
    NoBikeInfo,
    /// Vehicle being used on this particular trip can accommodate at least one bicycle
    AtLeastOneBike,
    /// No bicycles are allowed on this trip
    NoBikesAllowed,
    /// An unknown value not in the specification
    Unknown(i16),
}

impl<'de> Deserialize<'de> for BikesAllowedType {
    fn deserialize<D>(deserializer: D) -> Result<BikesAllowedType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Ok(match s {
            "" | "0" => BikesAllowedType::NoBikeInfo,
            "1" => BikesAllowedType::AtLeastOneBike,
            "2" => BikesAllowedType::NoBikesAllowed,
            s => BikesAllowedType::Unknown(s.parse().map_err(|_| {
                serde::de::Error::custom(format!(
                    "invalid value for BikeAllowedType, must be an integer: {s}"
                ))
            })?),
        })
    }
}

impl Serialize for BikesAllowedType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Note: for extended route type, we might loose the initial precise route type
        serialize_i16_as_str(
            serializer,
            match self {
                BikesAllowedType::NoBikeInfo => 0,
                BikesAllowedType::AtLeastOneBike => 1,
                BikesAllowedType::NoBikesAllowed => 2,
                BikesAllowedType::Unknown(i) => *i,
            },
        )
    }
}

/// Defines where a [FareAttribute] can be paid
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
pub enum PaymentMethod {
    /// Fare is paid on board
    #[serde(rename = "0")]
    Aboard,
    /// Fare must be paid before boarding
    #[serde(rename = "1")]
    PreBoarding,
}

/// Defines if the [Frequency] is exact (the vehicle runs exactly every n minutes) or not
#[derive(Debug, Serialize, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ExactTimes {
    /// Frequency-based trips
    FrequencyBased = 0,
    /// Schedule-based trips with the exact same headway throughout the day.
    ScheduleBased = 1,
}

impl<'de> Deserialize<'de> for ExactTimes {
    fn deserialize<D>(deserializer: D) -> Result<ExactTimes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Ok(match s {
            "" | "0" => ExactTimes::FrequencyBased,
            "1" => ExactTimes::ScheduleBased,
            &_ => {
                return Err(serde::de::Error::custom(format!(
                    "Invalid value `{s}`, expected 0 or 1"
                )));
            }
        })
    }
}

/// Defines how many transfers can be done with on [FareAttribute]
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Transfers {
    /// Unlimited transfers are permitted
    #[default]
    Unlimited,
    /// No transfers permitted on this fare
    NoTransfer,
    /// Riders may transfer once
    UniqueTransfer,
    ///Riders may transfer twice
    TwoTransfers,
    /// Other transfer values
    Other(i16),
}

impl<'de> Deserialize<'de> for Transfers {
    fn deserialize<D>(deserializer: D) -> Result<Transfers, D::Error>
    where
        D: Deserializer<'de>,
    {
        let i = Option::<i16>::deserialize(deserializer)?;
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
            Transfers::NoTransfer => serialize_i16_as_str(serializer, 0),
            Transfers::UniqueTransfer => serialize_i16_as_str(serializer, 1),
            Transfers::TwoTransfers => serialize_i16_as_str(serializer, 2),
            Transfers::Other(a) => serialize_i16_as_str(serializer, *a),
            Transfers::Unlimited => serializer.serialize_none(),
        }
    }
}
/// Defines the type of a [StopTransfer]
#[derive(Debug, Serialize, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TransferType {
    /// Recommended transfer point between routes
    #[serde(rename = "0")]
    #[default]
    Recommended,
    /// Departing vehicle waits for arriving one
    #[serde(rename = "1")]
    Timed,
    /// Transfer requires a minimum amount of time between arrival and departure to ensure a connection.
    #[serde(rename = "2")]
    MinTime,
    /// Transfer is not possible at this location
    #[serde(rename = "3")]
    Impossible,
    /// Passengers can stay onboard the same vehicle to transfer from one trip to another
    #[serde(rename = "4")]
    StayOnBoard,
    /// In-seat transfers aren't allowed between sequential trips.
    /// The passenger must alight from the vehicle and re-board.
    #[serde(rename = "5")]
    MustAlight,
}

impl<'de> Deserialize<'de> for TransferType {
    fn deserialize<D>(deserializer: D) -> Result<TransferType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Ok(match s {
            "" | "0" => TransferType::Recommended,
            "1" => TransferType::Timed,
            "2" => TransferType::MinTime,
            "3" => TransferType::Impossible,
            "4" => TransferType::StayOnBoard,
            "5" => TransferType::MustAlight,
            s => {
                return Err(serde::de::Error::custom(format!(
                    "Invalid value `{s}`, expected 0, 1, 2, 3, 4, 5"
                )));
            }
        })
    }
}

/// Type of pathway between [from_stop] and [to_stop]
#[derive(Debug, Serialize, Deserialize, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PathwayMode {
    /// A walkway
    #[serde(rename = "1")]
    #[default]
    Walkway,
    /// Stairs
    #[serde(rename = "2")]
    Stairs,
    /// Moving sidewalk / travelator
    #[serde(rename = "3")]
    MovingSidewalk,
    /// Escalator
    #[serde(rename = "4")]
    Escalator,
    /// Elevator
    #[serde(rename = "5")]
    Elevator,
    /// A pathway that crosses into an area of the station where a
    /// proof of payment is required (usually via a physical payment gate)
    #[serde(rename = "6")]
    FareGate,
    /// Indicates a pathway exiting an area where proof-of-payment is required
    /// into an area where proof-of-payment is no longer required.
    #[serde(rename = "7")]
    ExitGate,
}

/// Indicates in which direction the pathway can be used
#[derive(Debug, Serialize, Deserialize, Default, Copy, Clone, PartialEq, Eq, Hash)]
pub enum PathwayDirectionType {
    /// Unidirectional pathway, it can only be used from [from_stop_id] to [to_stop_id].
    #[serde(rename = "0")]
    #[default]
    Unidirectional,
    /// Bidirectional pathway, it can be used in the two directions.
    #[serde(rename = "1")]
    Bidirectional,
}

/// Defines the type of a [FareMedia]
#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
pub enum FareMediaType {
    /// Used when there is no fare media involved in purchasing or validating a fare product
    #[serde(rename = "0")]
    None,
    /// Physical paper ticket
    #[serde(rename = "1")]
    PhysicalPaperTicket,
    /// Physical transit card
    #[serde(rename = "2")]
    PhysicalTransitCard,
    /// cEMV (contactless Europay, Mastercard and Visa)
    #[serde(rename = "3")]
    CEmv,
    /// Mobile app
    #[serde(rename = "4")]
    MobileApp,
}

/// Specifies if an entry in rider_categories.txt should be considered the default category
#[derive(Debug, Serialize, Copy, Clone, PartialEq, Eq, Hash)]
pub enum DefaultFareCategory {
    /// Category is not considered the default.
    NotDefault = 0,
    /// Category is considered the default one.
    Default = 1,
}

impl<'de> Deserialize<'de> for DefaultFareCategory {
    fn deserialize<D>(deserializer: D) -> Result<DefaultFareCategory, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&str>::deserialize(deserializer)?;
        Ok(match s {
            "" | "0" => DefaultFareCategory::NotDefault,
            "1" => DefaultFareCategory::Default,
            &_ => {
                return Err(serde::de::Error::custom(format!(
                    "Invalid value `{s}`, expected 0 or 1"
                )));
            }
        })
    }
}

/// Specifies whether tickets can be bought for this item
#[derive(Debug, Default, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
pub enum TicketingType {
    /// If a ticketing_deep_link_id is set, tickets are available
    #[default]
    #[serde(rename = "0")]
    Available,
    /// Tickets are unavailable
    #[serde(rename = "1")]
    Unavailable,
}
