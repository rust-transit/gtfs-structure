use gtfs_structures::RawGtfs;

fn main() {
    let raw_gtfs = RawGtfs::new("fixtures/basic").expect("impossible to read gtfs");

    raw_gtfs.print_stats();

    for stop in raw_gtfs.stops.expect("impossible to read stops.txt") {
        println!("stop: {}", stop.name);
    }
}
