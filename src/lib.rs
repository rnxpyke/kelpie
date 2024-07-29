pub mod series;
pub mod store;

#[cfg(test)]
extern crate quickcheck;

#[cfg(test)]
#[macro_use]
extern crate quickcheck_macros;

use std::{
    collections::{BTreeMap, HashMap},
    i64,
};

pub use series::{Chunk, DataPoint, DecompressError, RawSeries};
pub use store::{ChunkMeta, GetChunkError, KelpieChunkStore, SetChunkError, SqliteChunkStore};

#[derive(Debug)]
pub struct Series {
    schedule: Schedule,
    data: RawSeries,
}

#[derive(Copy, Clone, Debug)]
pub struct Schedule {
    chunk_start: i64,
    chunk_end: i64,
}

impl Schedule {
    fn contains(&self, time: i64) -> bool {
        (self.chunk_start..self.chunk_end).contains(&time)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ScheduleConfig {
    // the chunk size in key space.
    // a chunk with chunk_size c and start s contains
    // values between s..s+c
    // should never be negative
    chunk_size: i64,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            chunk_size: 60 * 60 * 1000,
        }
    }
}

impl ScheduleConfig {
    fn init_schedule_from_time(&self, point: i64) -> Schedule {
        // implicitly round down
        let chunk_start = point / self.chunk_size * self.chunk_size;
        let chunk_end = chunk_start.saturating_add(self.chunk_size);
        Schedule {
            chunk_start,
            chunk_end,
        }
    }
}

pub enum InsertStatus {
    CompactmentPending(Schedule),
    Cached,
}

impl Series {
    fn new(schedule: Schedule) -> Self {
        let data = RawSeries::new();
        Self { data, schedule }
    }

    fn try_insert(&mut self, data_point: DataPoint) -> bool {
        if self.schedule.contains(data_point.time) {
            self.data.insert(data_point);
            return true;
        }
        false
    }
}

pub struct Kelpie {
    chunk_store: SqliteChunkStore,
    series: HashMap<i64, Series>,
    schedule_config: ScheduleConfig,
}

pub struct KelpieFake {
    series: HashMap<i64, RawSeries>,
}

impl Default for KelpieFake {
    fn default() -> Self {
        Self::new()
    }
}

impl KelpieFake {
    pub fn new() -> Self {
        Self {
            series: HashMap::new(),
        }
    }

    pub fn insert(&mut self, series_key: i64, data_point: DataPoint) {
        if data_point.value.is_nan() {
            return;
        }
        if data_point.time < 0 {
            return;
        }
        // because we work with exclusive ranges in the other impl, we skip here
        // we don't want to hit max timestamps anyways
        if data_point.time == i64::MAX {
            return;
        }
        let series = self.series.entry(series_key).or_default();
        series.data.insert(data_point.time, data_point.value);
    }

    pub fn query(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<RawSeries, GetChunkError> {
        if start > stop {
            return Ok(RawSeries::new());
        }
        let series = if let Some(s) = self.series.get(&series_key) {
            s
        } else {
            return Ok(RawSeries::new());
        };
        let range = series.data.range(start..stop);
        let map = BTreeMap::from_iter(range.map(|(&k, &v)| (k, v)));
        Ok(RawSeries { data: map })
    }
}

impl Kelpie {
    pub fn new_memory() -> Result<Self, sqlite::Error> {
        let chunk_store = SqliteChunkStore::new_memory()?;
        let series = HashMap::new();
        Ok(Self {
            chunk_store,
            series,
            schedule_config: ScheduleConfig::default(),
        })
    }

    pub fn query_exact_chunk(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<Option<(ChunkMeta, RawSeries)>, GetChunkError> {
        // check cache first
        if let Some(series) = self.series.get(&series_key) {
            // end checks should not be required because constant chunk siszes
            if series.schedule.chunk_start == start {
                let meta = ChunkMeta {
                    series_key,
                    start,
                    stop,
                };
                let series = series.data.clone();
                return Ok(Some((meta, series)));
            }
        }

        if let Some((meta, chunk)) = self.chunk_store.get_chunk(series_key, start, stop)? {
            let series = chunk.decompress().unwrap();
            return Ok(Some((meta, series)));
        }
        Ok(None)
    }

    pub fn query(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<RawSeries, GetChunkError> {
        let mut map = BTreeMap::new();
        let mut cur_start = start;
        while cur_start < stop {
            let cur_chunk = self.schedule_config.init_schedule_from_time(cur_start);
            let closest =
                self.query_exact_chunk(series_key, cur_chunk.chunk_start, cur_chunk.chunk_end)?;
            match closest {
                Some((_meta, mut chunk)) => {
                    map.append(&mut chunk.data);
                    cur_start = cur_chunk.chunk_end;
                }
                None => cur_start = cur_chunk.chunk_end,
            }
        }

        // cleanup any leftovers from unaligned chunks
        map.retain(|&k, _v| start <= k && k < stop);
        Ok(RawSeries { data: map })
    }

    pub fn new_path<A: AsRef<std::path::Path>>(path: A) -> Result<Self, sqlite::Error> {
        let chunk_store = SqliteChunkStore::new_path(path)?;
        let series = HashMap::new();
        Ok(Self {
            chunk_store,
            series,
            schedule_config: ScheduleConfig::default(),
        })
    }

    fn save_series(&mut self, series_key: i64) {
        let Some(series) = self.series.remove(&series_key) else {
            return;
        };
        let chunk = Chunk::compress_series(&series.data);
        let Schedule {
            chunk_start: start,
            chunk_end: stop,
        } = series.schedule;
        self.chunk_store
            .set_chunk(series_key, start, stop, &chunk)
            .unwrap();
    }

    fn load_series(&mut self, series_key: i64, schedule: Schedule) {
        self.save_series(series_key);
        let chunk_res = self
            .chunk_store
            .get_chunk(series_key, schedule.chunk_start, schedule.chunk_end)
            .unwrap();
        match chunk_res {
            Some((_meta, chunk)) => {
                let raw_series = chunk.decompress().unwrap();
                let series = Series {
                    schedule,
                    data: raw_series,
                };
                self.series.insert(series_key, series);
            }
            None => {
                self.series.insert(series_key, Series::new(schedule));
            }
        }
    }

    fn ensure_series_for(&mut self, series_key: i64, time: i64) {
        if let Some(series) = self.series.get_mut(&series_key) {
            if series.schedule.contains(time) {
                return;
            }
        }
        let schedule = self.schedule_config.init_schedule_from_time(time);
        self.load_series(series_key, schedule);
    }

    pub fn insert(&mut self, series_key: i64, data_point: DataPoint) {
        if data_point.value.is_nan() {
            return;
        }
        if data_point.time < 0 {
            return;
        }
        // skip max value because last chunk will go from last_multiple to max_value exclusive,
        // so we can never store max value
        if data_point.time == i64::MAX {
            return;
        }
        self.ensure_series_for(series_key, data_point.time);
        let series = self.series.get_mut(&series_key).unwrap();
        assert!(series.try_insert(data_point));
    }
}

#[cfg(test)]
mod tests {
    use core::f64;
    use std::io::Write;

    use super::*;
    use q_compress::CompressorConfig;
    use q_compress::DecompressorConfig;
    use quickcheck::quickcheck;
    use quickcheck::Arbitrary;

    #[test]
    fn should_create() -> Result<(), Box<dyn std::error::Error>> {
        let _kelpie = Kelpie::new_memory()?;
        Ok(())
    }

    #[test]
    fn should_insert() -> Result<(), Box<dyn std::error::Error>> {
        use rand::prelude::*;
        use rand::rngs::SmallRng;

        let mut kelpie = Kelpie::new_memory()?;
        let mut rng = SmallRng::seed_from_u64(0xdeadbeef);
        // float series
        const SERIES: usize = 64;
        let mut times: [i64; SERIES] = [1722180250000; SERIES];
        let mut vals: [f64; SERIES] = [0.0; SERIES];
        let val_variance = 20.0;
        for _ in 0..100_000 {
            let series_key = rng.gen_range(0..SERIES);
            let last_time = &mut times[series_key];
            let last_val = &mut vals[series_key];

            let time_inc = rng.gen_range(400..1100);
            *last_time += time_inc;
            let time = *last_time;

            let val_inc = rng.gen_range(-val_variance..val_variance);
            *last_val += val_inc;

            // truncate precision of floats to simulate less random measurements
            let value = f64::from_bits(last_val.to_bits() & (!0 << 36));
            assert!((*last_val - value).abs() < 1.0);
            let point = DataPoint { time, value };

            kelpie.insert(series_key as i64, point);
        }
        Ok(())
    }

    #[derive(Debug, Clone)]
    enum Cmd {
        Insert {
            series_key: i64,
            point: DataPoint,
        },
        Query {
            series_key: i64,
            start: i64,
            stop: i64,
        },
    }

    impl quickcheck::Arbitrary for Cmd {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let series_key = u8::arbitrary(g) as i64;
            let variant: u8 = u8::arbitrary(g);
            match variant {
                0 => {
                    let start = Arbitrary::arbitrary(g);
                    let size = u16::arbitrary(g);
                    Cmd::Query {
                        series_key,
                        start,
                        stop: start.saturating_add(size as i64),
                    }
                }
                _ => {
                    let points = Arbitrary::arbitrary(g);
                    Cmd::Insert {
                        series_key,
                        point: points,
                    }
                }
            }
        }

        /*
        fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
            match self {
                &Cmd::Insert { series_key, point } => {
                    let iter = DataPoint::shrink(&point).map(|point| Cmd::Insert { series_key: 0, point })
                        .chain(DataPoint::shrink(&point).map(move |point| Cmd::Insert { series_key, point: point }));
                    Box::new(iter)
                },
                &Cmd::Query { series_key, start, stop } => {
                    let iter = iter::once(Cmd::Query { series_key: 0, start, stop });
                    Box::new(iter)
                },
            }
        }
        */
    }

    /*
    impl Cmd {
        fn get_series_key(&self) -> i64 {
            match self {
                Cmd::Insert { series_key, .. } => *series_key,
                Cmd::Query { series_key, .. } => *series_key,
            }
        }
    }
    */

    fn kelpie_eq_fake(cmds: &[Cmd]) -> Result<(), Box<dyn std::error::Error>> {
        let mut kelpie = Kelpie::new_memory()?;
        let mut fake = KelpieFake::new();
        for cmd in cmds {
            match *cmd {
                Cmd::Insert { series_key, point } => {
                    kelpie.insert(series_key, point);
                    fake.insert(series_key, point);
                }
                Cmd::Query {
                    series_key,
                    start,
                    stop,
                } => {
                    let kelpie_res = kelpie.query(series_key, start, stop)?;
                    let fake_res = fake.query(series_key, start, stop)?;
                    if kelpie_res != fake_res {
                        Err("not matching")?;
                    }
                }
            }
        }
        Ok(())
    }

    /*
    trait Bisect: Sized + Copy {
        fn midpoint(min: Self, max: Self) -> Option<Self>;
        fn binary_search(mut min: Self, mut max: Self, pred: impl Fn(Self) -> bool) -> Self {
            loop {
                if let Some(midpoint) = Bisect::midpoint(min, max) {
                    if pred(midpoint) {
                        max = midpoint;
                    } else {
                        min = midpoint;
                    }
                } else {
                    return min;
                }
            }
        }
    }

    impl Bisect for i64 {
        fn midpoint(min: Self, max: Self) -> Option<Self> {
            if min >= max {
                return None;
            }
            let half = (max - min) / 2;
            if half == 0 {
                return None;
            }
            return Some(min + half);
        }
    }
    */

    #[test]
    fn it_should_match_exact_query() -> Result<(), Box<dyn std::error::Error>> {
        use Cmd::*;
        fn make_cmds(x: i64) -> Vec<Cmd> {
            let series_key = 0;
            let cmds = vec![
                Insert {
                    series_key,
                    point: DataPoint {
                        time: x,
                        value: 0.0,
                    },
                },
                Query {
                    series_key,
                    start: x - 1,
                    stop: x + 1,
                },
            ];
            cmds
        }

        let point = 3600000;

        kelpie_eq_fake(&make_cmds(point - 1))?;
        kelpie_eq_fake(&make_cmds(point))?;
        kelpie_eq_fake(&make_cmds(point + 1))?;
        Ok(())
    }

    #[test]
    fn it_should_match_huge_query() -> Result<(), Box<dyn std::error::Error>> {
        use Cmd::*;
        fn make_cmds(x: i64) -> Vec<Cmd> {
            let series_key = 0;
            let cmds = vec![
                Insert {
                    series_key,
                    point: DataPoint {
                        time: x,
                        value: 0.0,
                    },
                },
                Query {
                    series_key,
                    start: 0,
                    stop: 10000000,
                },
            ];
            cmds
        }

        kelpie_eq_fake(&make_cmds(3600000 - 100))?;
        kelpie_eq_fake(&make_cmds(3600000 - 1))?;
        kelpie_eq_fake(&make_cmds(3600000))?;
        kelpie_eq_fake(&make_cmds(3600000 + 1))?;
        kelpie_eq_fake(&make_cmds(3600000 + 100))?;
        Ok(())
    }

    #[test]
    fn it_should_match_big_forward_diff() -> Result<(), Box<dyn std::error::Error>> {
        use Cmd::*;
        fn make_cmds(x: i64) -> Vec<Cmd> {
            let series_key = 0;
            let cmds = vec![
                Insert {
                    series_key,
                    point: DataPoint {
                        time: 0,
                        value: 0.0,
                    },
                },
                Query {
                    series_key,
                    start: 0,
                    stop: 1,
                },
                Insert {
                    series_key,
                    point: DataPoint {
                        time: x,
                        value: 0.0,
                    },
                },
                Query {
                    series_key,
                    start: 0,
                    stop: 1,
                },
                Query {
                    series_key,
                    start: x,
                    stop: x + 1,
                },
            ];
            cmds
        }

        kelpie_eq_fake(&make_cmds(0))?;
        kelpie_eq_fake(&make_cmds(1))?;
        kelpie_eq_fake(&make_cmds(2))?;
        kelpie_eq_fake(&make_cmds(100))?;
        kelpie_eq_fake(&make_cmds(1000))?;
        kelpie_eq_fake(&make_cmds(3600000 - 1))?;
        kelpie_eq_fake(&make_cmds(3600000))?;
        kelpie_eq_fake(&make_cmds(3600000 + 1))?;
        kelpie_eq_fake(&make_cmds(3600000 * 2))?;
        Ok(())
    }

    #[test]
    fn it_should_match_huge_reverse_diff() -> Result<(), Box<dyn std::error::Error>> {
        use Cmd::*;
        fn make_cmds(x: i64) -> Vec<Cmd> {
            let series_key = 0;
            let cmds = vec![
                Insert {
                    series_key,
                    point: DataPoint {
                        time: x,
                        value: 0.0,
                    },
                },
                Query {
                    series_key,
                    start: x,
                    stop: x + 1,
                },
                Insert {
                    series_key,
                    point: DataPoint {
                        time: 0,
                        value: 0.0,
                    },
                },
                Query {
                    series_key,
                    start: x,
                    stop: x + 1,
                },
                Query {
                    series_key,
                    start: 0,
                    stop: 1,
                },
            ];
            cmds
        }

        kelpie_eq_fake(&make_cmds(0))?;
        kelpie_eq_fake(&make_cmds(1))?;
        kelpie_eq_fake(&make_cmds(2))?;
        kelpie_eq_fake(&make_cmds(100))?;
        kelpie_eq_fake(&make_cmds(1000))?;
        kelpie_eq_fake(&make_cmds(3600000 - 1))?;
        kelpie_eq_fake(&make_cmds(3600000))?;
        kelpie_eq_fake(&make_cmds(3600000 + 1))?;
        kelpie_eq_fake(&make_cmds(3600000 * 2))?;
        Ok(())
    }

    #[test]
    fn should_survie_fuzz1_test() -> Result<(), Box<dyn std::error::Error>> {
        use Cmd::*;
        let cmds = vec![
            Insert {
                series_key: -1,
                point: DataPoint {
                    time: -1,
                    value: -8.914951611789743e303,
                },
            },
            Query {
                series_key: -1,
                start: -1,
                stop: 0,
            },
        ];
        kelpie_eq_fake(&cmds)?;
        Ok(())
    }

    #[test]
    fn should_survie_fuzz2_test() -> Result<(), Box<dyn std::error::Error>> {
        use Cmd::*;
        let cmds = vec![Insert {
            series_key: -1086626725888,
            point: DataPoint {
                time: 9223372036854775807,
                value: 1.22e-321,
            },
        }];
        kelpie_eq_fake(&cmds)?;
        Ok(())
    }

    #[quickcheck]
    fn matches_fake(cmds: Vec<Cmd>) -> bool {
        kelpie_eq_fake(&cmds).is_ok()
    }

    /*

    */

    #[test]
    fn input1_that_kills_qcompress() {
        use Cmd::*;
        type Point = DataPoint;
        let series_key = 0;
        // these for vals when compressed using our qcompress settings trigger subtract with overflow in qcompress
        let vals = [
            2.8170090551184303e209,
            4.2984146959204563e204,
            2.8170090551184244e209,
            2.773899791842187e209,
        ];
        let cmds = vec![
            Insert {
                series_key,
                point: Point {
                    time: 0,
                    value: vals[0],
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 1,
                    value: vals[1],
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 2,
                    value: vals[2],
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 3,
                    value: vals[3],
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 3600000,
                    value: 0.0,
                },
            },
        ];
        kelpie_eq_fake(&cmds).unwrap();
    }

    #[test]
    fn input2_that_kills_qcompress() {
        use Cmd::*;
        type Point = DataPoint;
        let series_key = 0;
        let chunk_size = 60 * 60 * 1000;
        let vals = [
            2.8170090551184303e209,
            2.8169933786932377e209,
            2.8170090551184303e209,
            -2.8170090551184303e209,
            2.817009055114319e209,
        ];
        let cmds = vec![
            Insert {
                series_key,
                point: Point {
                    time: 0,
                    value: vals[0],
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 1,
                    value: vals[1],
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 2,
                    value: vals[2],
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 3,
                    value: vals[3],
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 4,
                    value: vals[4],
                },
            },
            Insert {
                series_key: 0,
                point: Point {
                    time: chunk_size,
                    value: 0.0,
                },
            },
        ];
        kelpie_eq_fake(&cmds).unwrap();
    }

    #[test]
    fn input3_that_kills_qcompress() {
        use Cmd::*;
        type Point = DataPoint;
        let series_key: i64 = 0;
        let a = f64::from_bits(0x8000000000004000);
        let b = f64::from_bits(0x8000000000000000);
        let cmds = vec![
            Insert {
                series_key,
                point: Point { time: 0, value: a },
            },
            Insert {
                series_key,
                point: Point { time: 1, value: -a },
            },
            Insert {
                series_key,
                point: Point { time: 2, value: b },
            },
            Insert {
                series_key,
                point: Point {
                    time: 60 * 60 * 1000 + 1,
                    value: 0.0,
                },
            },
            Insert {
                series_key,
                point: Point {
                    time: 0,
                    value: 0.0,
                },
            },
        ];
        kelpie_eq_fake(&cmds).unwrap();
    }

    #[test]
    fn qcompress_demo() {
        use q_compress::{Compressor, CompressorConfig, Decompressor, DecompressorConfig};
        let a = f64::from_bits(0x8000000000004000);
        let b = f64::from_bits(0x8000000000000000);
        let cfg = CompressorConfig::default()
            .with_use_gcds(false)
            .with_delta_encoding_order(1)
            .with_compression_level(8);
        let mut compressor = Compressor::<f64>::from_config(cfg);
        let compressed = compressor.simple_compress(&[a, -a, b]);
        let mut decompressor: Decompressor<f64> =
            Decompressor::from_config(DecompressorConfig::default());
        decompressor.write_all(&compressed).unwrap();
        let decompressed = decompressor.simple_decompress().unwrap();
        assert_eq!(&[a, -a, b], decompressed.as_slice());
    }
}
