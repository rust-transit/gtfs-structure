use gtfs_structures::Gtfs;

fn main() {
    /* Gtfs::new will try to guess if you provide a path, a local zip file or a remote zip file.
       You can also use Gtfs::from_path, Gtfs::from_url
    */
    let gtfs = Gtfs::new("fixtures/basic").expect("impossible to read gtfs");

    gtfs.print_stats();

    println!("there are {} stops in the gtfs", gtfs.stops.len());

    let route_1 = gtfs.routes.get("1").expect("no route 1");
    println!("{}: {:?}", route_1.short_name, route_1);

    let trip = gtfs
        .trips
        .get("trip1")
        .expect("impossible to find trip trip1");

    let stop_time = trip
        .stop_times
        .iter()
        .next()
        .expect("no stop times in trips");

    let stop = gtfs
        .stops
        .get(stop_time.stop)
        .expect("no stop in stop time");

    println!("first stop of trip 'trip1': {}", &stop.name);
}
