pub mod series;
pub mod store;

use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
};

pub use series::{Chunk, DataPoint, DecompressError, RawSeries};
pub use store::{ChunkMeta, GetChunkError, KelpieChunkStore, SetChunkError, SqliteChunkStore};

pub struct Series {
    schedule: Option<Schedule>,
    schedule_config: ScheduleConfig,
    data: RawSeries,
}

#[derive(Copy, Clone, Debug)]
pub struct Schedule {
    chunk_start: i64,
    chunk_end: i64,
}

#[derive(Copy, Clone, Debug)]
pub struct ScheduleConfig {
    // the chunk size in key space.
    // a chunk with chunk_size c and start s contains
    // values between s..s+c
    // should never be negative
    chunk_size: i64,

    // how long to wait before compacting the series
    // also in key space
    // should never be negative
    compact_after: i64,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            chunk_size: 60 * 60 * 1000,
            compact_after: 15 * 60 * 1000,
        }
    }
}

impl ScheduleConfig {
    fn init_schedule_from_time(&self, point: i64) -> Schedule {
        // implicitly round down
        let chunk_start = point / self.chunk_size;
        let chunk_end = chunk_start + self.chunk_size;
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
    fn new(config: ScheduleConfig) -> Self {
        let data = RawSeries::new();
        Self {
            data,
            schedule: None,
            schedule_config: config,
        }
    }

    fn insert(&mut self, data_point: DataPoint) -> InsertStatus {
        const EXPECT_MSG: &'static str = "We just inserted a data point, this can't be emtpy";
        self.data.insert(data_point);
        let first_time = self.data.first_time().expect(EXPECT_MSG);
        if self.schedule.is_none() {
            self.schedule = Some(self.schedule_config.init_schedule_from_time(first_time));
        }
        let last_time = self.data.last_time().expect(EXPECT_MSG);
        let schedule = self.schedule.expect(EXPECT_MSG);
        if last_time > schedule.chunk_end + self.schedule_config.compact_after {
            return InsertStatus::CompactmentPending(schedule);
        }
        return InsertStatus::Cached;
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

impl KelpieFake {
    pub fn new() -> Self {
        Self {
            series: HashMap::new(),
        }
    }

    pub fn insert(&mut self, series_key: i64, data_point: DataPoint) {
        let series = self.series.entry(series_key).or_default();
        series.data.insert(data_point.time, data_point.value);
    }

    pub fn query(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<RawSeries, GetChunkError> {
        let series = if let Some(s) = self.series.get(&series_key) {
            s
        } else {
            return Ok(RawSeries::new());
        };
        let range = series.data.range(start..stop);
        let map = BTreeMap::from_iter(range.map(|(&k, &v)| (k, v)));
        return Ok(RawSeries { data: map });
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

    pub fn query_closest_chunk(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<Option<(ChunkMeta, RawSeries)>, GetChunkError> {
        // check cache first
        if let Some(series) = self.series.get(&series_key) {
            if let Some(schedule) = series.schedule {
                if schedule.chunk_start <= start && schedule.chunk_end >= start {
                    let end = std::cmp::min(stop, schedule.chunk_end);
                    let meta = ChunkMeta {
                        series_key,
                        start,
                        stop: end,
                    };
                    let range = series.data.data.range(start..end);
                    let map = BTreeMap::from_iter(range.map(|(&k, &v)| (k, v)));
                    let series = RawSeries { data: map };
                    return Ok(Some((meta, series)));
                }
            }
        }

        if let Some((meta, chunk)) = self.chunk_store.get_chunk(series_key, start, stop)? {
            let series = chunk.decompress().unwrap();
            return Ok(Some((meta, series)));
        }
        return Ok(None);
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
            match self.query_closest_chunk(series_key, cur_start, stop)? {
                Some((meta, mut chunk)) => {
                    map.append(&mut chunk.data);
                    // this should not happen, this break is just here to prevent infinite loops,
                    // maybe turn into error
                    if meta.stop <= cur_start {
                        break;
                    }
                    cur_start = meta.stop;
                }
                None => break,
            }
        }

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

    fn compact(&mut self, series_key: i64, schedule: Schedule) -> Result<(), SetChunkError> {
        let series = if let Some(s) = self.series.get_mut(&series_key) {
            s
        } else {
            return Ok(());
        };

        let mut early_vals = {
            // after this, retained contains all keys >= schedule.chunk_end,
            // while the original series contains everything before.
            // therefore, we swap retained and series value to get everything old
            let mut retained = series.data.data.split_off(&schedule.chunk_end);
            std::mem::swap(&mut retained, &mut series.data.data);
            retained
        };

        // early vals is now every value with time < chunk_end.
        // it may still contain data that is before chunk start.
        // TODO: compact early data correctly.

        // Drop early values for now
        early_vals.retain(|&k, _v| k >= schedule.chunk_start);

        // set new schedule for remaining data
        if let Some(first_time) = series.data.first_time() {
            series.schedule = Some(self.schedule_config.init_schedule_from_time(first_time));
        }

        // compress chunk
        let chunk_series = RawSeries { data: early_vals };
        let chunk = Chunk::compress_series(&chunk_series);
        drop(chunk_series);

        // store chunk
        self.chunk_store
            .set_chunk(series_key, schedule.chunk_start, schedule.chunk_end, &chunk)?;
        Ok(())
    }

    pub fn insert(&mut self, series_key: i64, data_point: DataPoint) {
        let series = self
            .series
            .entry(series_key)
            .or_insert_with(|| Series::new(self.schedule_config));
        match series.insert(data_point) {
            InsertStatus::Cached => {}
            InsertStatus::CompactmentPending(schedule) => {
                self.compact(series_key, schedule).unwrap()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

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

    #[derive(Debug)]
    enum Cmd {
        Insert {
            series_key: i64,
            points: Vec<DataPoint>,
        },
        Query {
            series_key: i64,
            start: i64,
            stop: i64,
        },
    }

    impl Cmd {
        fn get_series_key(&self) -> i64 {
            match self {
                Cmd::Insert { series_key, .. } => *series_key,
                Cmd::Query { series_key, .. } => *series_key,
            }
        }

        fn gen_cmd<R: Rng>(
            rng: &mut R,
            series_key: i64,
            last_time: &mut i64,
            last_val: &mut f64,
            variance: f64,
        ) -> Self {
            match rng.gen_range(0..10) {
                // query
                0 => {
                    let start: i64 = *last_time + rng.gen_range(-10_000..10_000);
                    let size = rng.gen_range(1..(3600 * 2));
                    let stop = start + size;
                    Cmd::Query {
                        series_key: series_key as i64,
                        start,
                        stop,
                    }
                }
                // insert
                _ => {
                    let count = rng.gen_range(0..64);
                    let mut points = vec![];
                    for _ in 0..count {
                        let time_inc = rng.gen_range(400..1100);
                        *last_time += time_inc;
                        let time = *last_time;

                        let val_inc = rng.gen_range(-variance..variance);
                        *last_val += val_inc;

                        // truncate precision of floats to simulate less random measurements
                        let value = f64::from_bits(last_val.to_bits() & (!0 << 36));
                        assert!((*last_val - value).abs() < 1.0);
                        let point = DataPoint { time, value };
                        points.push(point);
                    }
                    Cmd::Insert {
                        series_key: series_key as i64,
                        points,
                    }
                }
            }
        }
    }

    fn kelpie_eq_fake(cmds: &[Cmd]) -> Result<(), Box<dyn std::error::Error>> {
        let mut kelpie = Kelpie::new_memory()?;
        let mut fake = KelpieFake::new();
        for cmd in cmds {
            dbg!(cmd);
            match cmd {
                &Cmd::Insert { series_key, ref points } => {
                    for &point in points {
                        kelpie.insert(series_key, point);
                        fake.insert(series_key, point);
                    }
                }
                &Cmd::Query {
                    series_key,
                    start,
                    stop,
                } => {
                    let kelpie_res = kelpie.query(series_key, start, stop)?;
                    let fake_res = fake.query(series_key, start, stop)?;
                    assert_eq!(kelpie_res, fake_res);
                }
            }
        }
        Ok(())
    }

    #[test] 
    fn it_should_match_fake_example() -> Result<(), Box<dyn std::error::Error>> {
        use Cmd::*;
        let cmds = vec![Insert {
            series_key: 44,
            points: vec![
                DataPoint {
                    time: 1722180250542,
                    value: 1.9446563720703125,
                },
                DataPoint {
                    time: 1722180251591,
                    value: -7.3121337890625,
                },
                DataPoint {
                    time: 1722180252348,
                    value: 3.3428955078125,
                },
                DataPoint {
                    time: 1722180253315,
                    value: 5.15826416015625,
                },
                DataPoint {
                    time: 1722180254180,
                    value: 20.19775390625,
                },
                DataPoint {
                    time: 1722180254720,
                    value: 23.616943359375,
                },
                DataPoint {
                    time: 1722180255534,
                    value: 19.058349609375,
                },
                DataPoint {
                    time: 1722180256630,
                    value: 0.9685516357421875,
                },
                DataPoint {
                    time: 1722180257237,
                    value: -7.38482666015625,
                },
                DataPoint {
                    time: 1722180257727,
                    value: -19.83203125,
                },
                DataPoint {
                    time: 1722180258480,
                    value: -23.98486328125,
                },
                DataPoint {
                    time: 1722180259575,
                    value: -26.640380859375,
                },
                DataPoint {
                    time: 1722180260532,
                    value: -16.98291015625,
                },
                DataPoint {
                    time: 1722180261335,
                    value: -18.25244140625,
                },
                DataPoint {
                    time: 1722180262266,
                    value: -20.137451171875,
                },
                DataPoint {
                    time: 1722180262859,
                    value: -2.533538818359375,
                },
                DataPoint {
                    time: 1722180263615,
                    value: 12.406982421875,
                },
                DataPoint {
                    time: 1722180264410,
                    value: -0.018148422241210938,
                },
                DataPoint {
                    time: 1722180265119,
                    value: -7.62786865234375,
                },
                DataPoint {
                    time: 1722180266041,
                    value: 1.5860137939453125,
                },
                DataPoint {
                    time: 1722180266930,
                    value: 0.8058090209960938,
                },
                DataPoint {
                    time: 1722180267830,
                    value: -14.1217041015625,
                },
                DataPoint {
                    time: 1722180268705,
                    value: -21.812744140625,
                },
                DataPoint {
                    time: 1722180269324,
                    value: -6.90911865234375,
                },
                DataPoint {
                    time: 1722180270274,
                    value: -15.9766845703125,
                },
                DataPoint {
                    time: 1722180270970,
                    value: -20.261474609375,
                },
                DataPoint {
                    time: 1722180271681,
                    value: -0.5149459838867188,
                },
                DataPoint {
                    time: 1722180272184,
                    value: -5.525146484375,
                },
                DataPoint {
                    time: 1722180273067,
                    value: 5.13946533203125,
                },
                DataPoint {
                    time: 1722180273875,
                    value: 23.054443359375,
                },
                DataPoint {
                    time: 1722180274337,
                    value: 25.7763671875,
                },
                DataPoint {
                    time: 1722180275281,
                    value: 24.168212890625,
                },
                DataPoint {
                    time: 1722180276304,
                    value: 15.8856201171875,
                },
                DataPoint {
                    time: 1722180276933,
                    value: 10.1092529296875,
                },
                DataPoint {
                    time: 1722180277447,
                    value: -6.6290283203125,
                },
                DataPoint {
                    time: 1722180278182,
                    value: -23.884033203125,
                },
                DataPoint {
                    time: 1722180278959,
                    value: -33.09765625,
                },
                DataPoint {
                    time: 1722180279967,
                    value: -20.55078125,
                },
                DataPoint {
                    time: 1722180280736,
                    value: -31.531005859375,
                },
                DataPoint {
                    time: 1722180281737,
                    value: -48.88037109375,
                },
                DataPoint {
                    time: 1722180282505,
                    value: -38.64404296875,
                },
                DataPoint {
                    time: 1722180283073,
                    value: -22.06103515625,
                },
                DataPoint {
                    time: 1722180283915,
                    value: -18.541259765625,
                },
                DataPoint {
                    time: 1722180284992,
                    value: -26.884033203125,
                },
                DataPoint {
                    time: 1722180285847,
                    value: -12.86767578125,
                },
                DataPoint {
                    time: 1722180286712,
                    value: -26.864013671875,
                },
                DataPoint {
                    time: 1722180287723,
                    value: -40.82470703125,
                },
                DataPoint {
                    time: 1722180288792,
                    value: -59.51953125,
                },
                DataPoint {
                    time: 1722180289217,
                    value: -54.57275390625,
                },
                DataPoint {
                    time: 1722180289811,
                    value: -72.6044921875,
                },
                DataPoint {
                    time: 1722180290681,
                    value: -71.1044921875,
                },
                DataPoint {
                    time: 1722180291147,
                    value: -70.412109375,
                },
                DataPoint {
                    time: 1722180292205,
                    value: -52.68115234375,
                },
            ],
        },
        Query {
            series_key: 44,
            start: 1722180284390,
            stop: 1722180286869,
        }];

        kelpie_eq_fake(&cmds)
    }

    #[test]
    fn it_should_match_fake() -> Result<(), Box<dyn std::error::Error>> {
        use rand::prelude::*;
        use rand::rngs::SmallRng;

        let mut rng = SmallRng::seed_from_u64(0xdeadbeef);

        const SERIES: usize = 64;
        let mut times: [i64; SERIES] = [1722180250000; SERIES];
        let mut vals: [f64; SERIES] = [0.0; SERIES];

        let mut cmds = vec![];
        for _ in 0..1000 {
            let series_key = rng.gen_range(0..SERIES);
            let cmd = Cmd::gen_cmd(
                &mut rng,
                series_key as i64,
                &mut times[series_key],
                &mut vals[series_key],
                20.0,
            );
            if cmd.get_series_key() == 44 {
                cmds.push(cmd);
            }
        }
        
        kelpie_eq_fake(&cmds)?;

        Ok(())
    }
}
