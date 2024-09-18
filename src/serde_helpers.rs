use chrono::NaiveDate;
use rgb::RGB8;
use serde::de::{self, Deserialize, Deserializer};
use serde::ser::Serializer;

pub fn deserialize_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    NaiveDate::parse_from_str(s, "%Y%m%d").map_err(serde::de::Error::custom)
}

pub fn serialize_date<S>(date: &NaiveDate, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&date.format("%Y%m%d").to_string())
}

pub fn deserialize_option_date<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Option::<&str>::deserialize(deserializer)?
        .map(|s| NaiveDate::parse_from_str(s, "%Y%m%d").map_err(serde::de::Error::custom));
    match s {
        Some(Ok(s)) => Ok(Some(s)),
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

pub fn serialize_option_date<S>(date: &Option<NaiveDate>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match date {
        None => serializer.serialize_none(),
        Some(d) => serialize_date(d, serializer),
    }
}

pub fn parse_time_impl(h: &str, m: &str, s: &str) -> Result<u32, std::num::ParseIntError> {
    let hours: u32 = h.parse()?;
    let minutes: u32 = m.parse()?;
    let seconds: u32 = s.parse()?;
    Ok(hours * 3600 + minutes * 60 + seconds)
}

pub fn parse_time(s: &str) -> Result<u32, crate::Error> {
    let mk_err = || crate::Error::InvalidTime(s.to_owned());

    if s.len() < 7 {
        Err(mk_err())
    } else {
        let mut parts = s.split(':');

        let hour = parts.next().ok_or_else(mk_err)?;
        let min = parts.next().ok_or_else(mk_err)?;
        let sec = parts.next().ok_or_else(mk_err)?;
        if parts.next().is_some() {
            return Err(mk_err());
        }

        if min.len() != 2 || sec.len() != 2 {
            return Err(mk_err());
        }

        parse_time_impl(hour, min, sec).map_err(|_| mk_err())
    }
}

pub fn deserialize_time<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    parse_time(s).map_err(de::Error::custom)
}

pub fn serialize_time<S>(time: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(
        format!(
            "{:02}:{:02}:{:02}",
            time / 3600,
            time % 3600 / 60,
            time % 60
        )
        .as_str(),
    )
}

pub fn deserialize_optional_time<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<&str> = Deserialize::deserialize(deserializer)?;

    match s {
        None => Ok(None),
        Some(t) => parse_time(t).map(Some).map_err(de::Error::custom),
    }
}

pub fn serialize_optional_time<S>(time: &Option<u32>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match time {
        None => serializer.serialize_none(),
        Some(t) => serialize_time(t, serializer),
    }
}

pub fn de_with_optional_float<'de, D>(de: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(de).and_then(|s| {
        if s.is_empty() {
            Ok(None)
        } else {
            s.parse().map(Some).map_err(de::Error::custom)
        }
    })
}

pub fn serialize_float_as_str<S>(float: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match float {
        None => serializer.serialize_str(""),
        Some(f) => serializer.serialize_str(&f.to_string()),
    }
}

pub fn parse_color(
    s: &str,
    default: impl std::ops::FnOnce() -> RGB8,
) -> Result<RGB8, crate::Error> {
    if s.is_empty() {
        return Ok(default());
    }
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

pub fn deserialize_route_color<'de, D>(de: D) -> Result<RGB8, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(de)
        .and_then(|s| parse_color(&s, default_route_color).map_err(de::Error::custom))
}

pub fn deserialize_route_text_color<'de, D>(de: D) -> Result<RGB8, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(de).and_then(|s| parse_color(&s, RGB8::default).map_err(de::Error::custom))
}

pub fn serialize_color<S>(color: &RGB8, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(format!("{:02X}{:02X}{:02X}", color.r, color.g, color.b).as_str())
}

pub fn default_route_color() -> RGB8 {
    RGB8::new(255, 255, 255)
}

pub fn de_with_empty_default<'de, T, D>(de: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(de).map(|opt| opt.unwrap_or_default())
}

pub fn deserialize_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s {
        "0" => Ok(false),
        "1" => Ok(true),
        &_ => Err(serde::de::Error::custom(format!(
            "Invalid value `{s}`, expected 0 or 1"
        ))),
    }
}

pub fn serialize_bool<S>(value: &bool, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u8(u8::from(*value))
}

#[test]
fn test_serialize_time() {
    #[derive(Serialize, Deserialize)]
    struct Test {
        #[serde(
            deserialize_with = "deserialize_time",
            serialize_with = "serialize_time"
        )]
        time: u32,
    }
    let data_in = "time\n01:01:01\n";
    let parsed: Test = csv::Reader::from_reader(data_in.as_bytes())
        .deserialize()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(3600 + 60 + 1, parsed.time);

    let data_in_long_ride = "time\n172:35:42\n";
    let parsed_long_ride: Test = csv::Reader::from_reader(data_in_long_ride.as_bytes())
        .deserialize()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!((172 * 3600) + (35 * 60) + 42, parsed_long_ride.time);

    let mut wtr = csv::Writer::from_writer(vec![]);
    wtr.serialize(parsed).unwrap();
    let data_out = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
    assert_eq!(data_in, data_out);
}
