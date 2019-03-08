use gtfs_structures::Gtfs;

fn main() {
    let gtfs = Gtfs::new("fixtures").expect("impossible to read gtfs");

    gtfs.print_stats();
}
