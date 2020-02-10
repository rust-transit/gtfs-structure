use thiserror::Error;

#[derive(Debug)]
pub struct LineError {
    pub headers: Vec<String>,
    pub values: Vec<String>,
}

/// An error that can occur when processing GTFS data.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Cound not find file {0}")]
    MissingFile(String),
    #[error("The id {0} is not known")]
    ReferenceError(String),
    #[error("Could not read GTFS: {0} is neither a file nor a directory")]
    NotFileNorDirectory(String),
    #[error("'{0}' is not a valid time")]
    InvalidTime(String),
    #[error("impossible to read file")]
    IO(#[from] std::io::Error),
    #[error("impossible to read '{file_name}'")]
    NamedFileIO {
        file_name: String,
        #[source]
        source: std::io::Error,
    },
    #[cfg(feature = "read-url")]
    #[error("impossible to remotely access file")]
    Fetch(#[from] reqwest::Error),
    #[error("impossible to read csv file '{file_name}'")]
    CSVError {
        file_name: String,
        #[source]
        source: csv::Error,
        line_in_error: Option<LineError>,
    },
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
}
