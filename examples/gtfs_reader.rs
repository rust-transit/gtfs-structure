fn main() {
    /* Gtfs::new will try to guess if you provide a path, a local zip file or a remote zip file.
       You can also use Gtfs::from_path, Gtfs::from_url
    */
    let mut gtfs = gtfs_structures::GtfsReader::default()
        .read_stop_times(true)
        .read("fixtures/basic")
        .expect("impossible to read gtfs");
    gtfs.print_stats();

    println!("there are {} stops in the gtfs", gtfs.stops.len());

    let route_1 = gtfs.routes.get("1").expect("no route 1");
    println!("{}: {:?}", route_1.short_name, route_1);

    // you can access a stop by a &str
    let _ = gtfs
        .get_stop_by_raw_id("stop1")
        .expect("unable to find stop Stop Area");

    let trip = gtfs.trips.get("trip1").expect("no route 1");
    let stop_id: &gtfs_structures::Id<gtfs_structures::Stop> =
        &trip.stop_times.first().expect("no stoptimes").stop;

    // or with a typed id if you have one

    // if no stops have been removed from the gtfs, you can safely access the stops by it's id
    let s = &gtfs.stops[stop_id];
    println!("stop name: {}", &s.name);

    // if some removal have been done, you can also you those method to get an Option<Stop>
    let s = gtfs.get_stop(stop_id).expect("this stop should exists");
    println!("stop description: {}", &s.description);

    // or you can access it via `stops.get`
    let s = gtfs.stops.get(stop_id).expect("this stop should exists");
    println!("stop location type: {:?}", &s.location_type);

    let mut s = gtfs
        .stops
        .get_mut(stop_id)
        .expect("this stop should exists");
    s.code = Some("code".into());
}
