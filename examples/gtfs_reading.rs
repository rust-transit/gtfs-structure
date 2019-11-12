use gtfs_structures::Gtfs;

fn main() {
    let gtfs = Gtfs::from_path("fixtures/basic").expect("impossible to read gtfs");

    gtfs.print_stats();

    println!("there are {} stops in the gtfs", gtfs.stops.len());

    let route_1 = gtfs.routes.get("1").expect("no route 1");
    println!("{}: {:?}", route_1.short_name, route_1);
}
