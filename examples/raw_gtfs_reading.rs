use gtfs_structures::RawGtfs;

fn main() {
    let raw_gtfs = RawGtfs::new("fixtures").expect("impossible to read gtfs");

    raw_gtfs.print_stats();
}
