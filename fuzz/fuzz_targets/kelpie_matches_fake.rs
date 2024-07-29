#![no_main]

use libfuzzer_sys::fuzz_target;
use kelpie::{Kelpie, KelpieFake, DataPoint};

use libfuzzer_sys::arbitrary;
use arbitrary::Arbitrary;


#[derive(Debug, Copy, Clone, Arbitrary)]
struct Point {
    time: i64,
    value: f64,
}

impl Into<DataPoint> for Point {
    fn into(self) -> DataPoint {
        DataPoint { time: self.time, value: self.value }
    }
}

#[derive(Debug, Clone, Arbitrary)]
enum Cmd {
    Insert {
        series_key: i64,
        point: Point,
    },
    Query {
        series_key: i64,
        start: i64,
        stop: i64,
    },
}

fuzz_target!(|data: Vec<Cmd>| {
    let mut kelpie = Kelpie::new_memory().unwrap();
    let mut fake = KelpieFake::new();
    for cmd in data.into_iter() {
        match cmd {
            Cmd::Insert { series_key, point } => {
                kelpie.insert(series_key, point.into());
                fake.insert(series_key, point.into());
            },
            Cmd::Query { series_key, start, stop } => {
                if stop < start { continue; }
                if (stop as i128 -start as i128) > 1000 * 60 * 60 * 24 { continue; }
                let kelpie_res = kelpie.query(series_key, start, stop).unwrap();
                let fake_res = fake.query(series_key, start, stop).unwrap();
                assert_eq!(kelpie_res, fake_res);
            }
        }
    }
});
