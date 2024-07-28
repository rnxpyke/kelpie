use std::{collections::BTreeMap, io::Write};

use q_compress::{errors::QCompressError, Decompressor, DecompressorConfig};

pub struct DataPoint {
    time: i64,
    value: f64,
}

#[derive(Debug, PartialEq)]
pub struct RawSeries {
    data: BTreeMap<i64, f64>,
}

impl Default for RawSeries {
    fn default() -> Self {
        Self::new()
    }
}

impl RawSeries {
    pub fn new() -> Self {
        RawSeries {
            data: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, point: DataPoint) {
        self.data.insert(point.time, point.value);
    }

    pub fn serial_size_hint(&self) -> usize {
        self.data.len() * (8 * 2)
    }
}

pub enum DecompressError {
    TimeHeaderMissing,
    TimesMissing,
    ValHeaderMissing,
    ValsMissing,
    DecompressError(QCompressError),
}

impl From<QCompressError> for DecompressError {
    fn from(value: QCompressError) -> Self {
        Self::DecompressError(value)
    }
}

fn raw_decompress(bytes: &[u8]) -> Result<RawSeries, DecompressError> {
    if bytes.len() < 8 {
        return Err(DecompressError::TimeHeaderMissing);
    }
    let times_len = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
    let times_end = times_len + 8;
    if bytes.len() < times_end {
        return Err(DecompressError::TimesMissing);
    }
    let compressed_times = &bytes[8..(8 + times_len)];
    if bytes.len() - times_end < 8 {
        return Err(DecompressError::ValHeaderMissing);
    }
    let vals_len =
        u64::from_le_bytes(bytes[times_end..(times_end + 8)].try_into().unwrap()) as usize;
    let vals_end = times_end + vals_len + 8;
    if bytes.len() < vals_end {
        return Err(DecompressError::ValsMissing);
    };
    let compressed_vals = &bytes[(times_end + 8)..vals_end];

    let times = {
        let mut decompressor: Decompressor<i64> =
            q_compress::Decompressor::from_config(DecompressorConfig::default());
        decompressor.write_all(compressed_times).unwrap();
        decompressor.simple_decompress()?
    };

    let values = {
        let mut decompressor: Decompressor<f64> =
            q_compress::Decompressor::from_config(DecompressorConfig::default());
        decompressor.write_all(compressed_vals).unwrap();
        decompressor.simple_decompress()?
    };

    let mut series = RawSeries::new();
    let mut i = 0;
    loop {
        if i >= times.len() {
            break;
        }
        if i >= values.len() {
            break;
        }
        series.insert(DataPoint {
            time: times[i],
            value: values[i],
        });
        i += 1;
    }
    Ok(series)
}

fn raw_compress(raw: &RawSeries) -> Vec<u8> {
    use q_compress::CompressorConfig;
    let compressed_times = {
        let cfg = CompressorConfig::default()
            .with_use_gcds(false)
            .with_compression_level(8)
            .with_delta_encoding_order(2);
        let timevec: Vec<i64> = raw.data.keys().copied().collect();

        q_compress::Compressor::from_config(cfg).simple_compress(&timevec)
    };
    let compressed_vals = {
        let cfg = CompressorConfig::default()
            .with_use_gcds(false)
            .with_delta_encoding_order(1)
            .with_compression_level(8);
        let timevec: Vec<f64> = raw.data.values().copied().collect();

        q_compress::Compressor::from_config(cfg).simple_compress(&timevec)
    };
    let mut res = vec![0u8; compressed_times.len() + 8 + compressed_vals.len() + 8];
    res[0..8].copy_from_slice(&compressed_times.len().to_le_bytes());
    let times_end = 8 + compressed_times.len();
    res[8..times_end].copy_from_slice(&compressed_times);
    res[times_end..(times_end + 8)].copy_from_slice(&compressed_vals.len().to_le_bytes());
    res[(times_end + 8)..].copy_from_slice(&compressed_vals);
    res
}

pub struct Chunk {
    pub(crate) compressed_data: Vec<u8>,
}

impl Chunk {
    pub fn decompress(&self) -> Result<RawSeries, DecompressError> {
        raw_decompress(&self.compressed_data)
    }

    pub fn compress_series(series: &RawSeries) -> Chunk {
        let chunk = Chunk {
            compressed_data: raw_compress(series),
        };
        chunk
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SetChunkError {
    #[error("Driver error")]
    Driver(#[from] Box<dyn std::error::Error>),
}

#[derive(thiserror::Error, Debug)]
pub enum GetChunkError {
    #[error("Driver error")]
    Driver(#[from] Box<dyn std::error::Error>),
}

pub trait KelpieChunkStore {
    fn get_chunk(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<Option<(ChunkMeta, Chunk)>, GetChunkError>;
    fn set_chunk(
        &mut self,
        series_key: i64,
        start: i64,
        stop: i64,
        chunk: &Chunk,
    ) -> Result<(), SetChunkError>;
}

pub struct SqliteChunkStore {
    db: sqlite::Connection,
}

impl SqliteChunkStore {
    fn migrate(db: &mut sqlite::Connection) -> Result<(), sqlite::Error> {
        db.execute("CREATE TABLE IF NOT EXISTS chunks (series INTEGER, start INTEGER, stop INTEGER, chunk BLOB)")?;
        Ok(())
    }

    pub fn new_memory() -> Result<Self, sqlite::Error> {
        let mut db = sqlite::open(":memory:")?;
        Self::migrate(&mut db)?;
        Ok(Self { db })
    }

    pub fn new_path<T: AsRef<std::path::Path>>(path: T) -> Result<Self, sqlite::Error> {
        let mut db = sqlite::open(path)?;
        Self::migrate(&mut db)?;
        Ok(Self { db })
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ChunkMeta {
    series_key: i64,
    start: i64,
    stop: i64,
}

impl KelpieChunkStore for SqliteChunkStore {
    fn get_chunk(
        &self,
        series_key: i64,
        start: i64,
        stop: i64,
    ) -> Result<Option<(ChunkMeta, Chunk)>, GetChunkError> {
        fn driver(e: sqlite::Error) -> GetChunkError {
            GetChunkError::Driver(e.into())
        }
        let mut statement = self.db.prepare("SELECT start, stop, chunk from chunks WHERE series == ? AND start <= ? AND stop >= ? ORDER BY start DESC").map_err(driver)?;

        statement.bind((1, series_key)).map_err(driver)?;
        statement.bind((2, start)).map_err(driver)?;
        statement.bind((3, stop)).map_err(driver)?;

        let mut res = None;
        while let sqlite::State::Row = statement.next().map_err(driver)? {
            let res_start: i64 = statement.read::<i64, _>("start").map_err(driver)?;
            let res_stop: i64 = statement.read("stop").map_err(driver)?;
            dbg!(res_start, res_stop);
            let res_chunk: Vec<u8> = statement.read("chunk").map_err(driver)?;
            let meta = ChunkMeta {
                series_key,
                start: res_start,
                stop: res_stop,
            };
            let chunk = Chunk {
                compressed_data: res_chunk,
            };
            res = Some((meta, chunk));
            break;
        }

        statement.reset().map_err(driver)?;
        Ok(res)
    }

    fn set_chunk(
        &mut self,
        series_key: i64,
        start: i64,
        stop: i64,
        chunk: &Chunk,
    ) -> Result<(), SetChunkError> {
        fn driver(e: sqlite::Error) -> SetChunkError {
            SetChunkError::Driver(e.into())
        }
        let mut statement = self
            .db
            .prepare("INSERT INTO chunks VALUES (?, ?, ?, ?)")
            .map_err(driver)?;
        statement.bind((1, series_key)).map_err(driver)?;
        statement.bind((2, start)).map_err(driver)?;
        statement.bind((3, stop)).map_err(driver)?;
        statement
            .bind((4, chunk.compressed_data.as_slice()))
            .map_err(driver)?;
        loop {
            let state = statement.next().map_err(driver)?;
            match state {
                sqlite::State::Row => {}
                sqlite::State::Done => break,
            }
        }
        statement.reset().map_err(driver)?;
        Ok(())
    }
}

pub struct KelpieSeries {
    start_timestamp: u64,
}

#[cfg(test)]
mod tests {
    use crate::{raw_compress, raw_decompress, Chunk, DataPoint, KelpieChunkStore, RawSeries};

    #[test]
    fn should_create_sqlite_chunk_store() -> Result<(), Box<dyn std::error::Error>> {
        let _store = super::SqliteChunkStore::new_memory()?;
        Ok(())
    }

    #[test]
    fn should_store_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = super::SqliteChunkStore::new_memory()?;
        let chunk = Chunk {
            compressed_data: vec![],
        };
        store.set_chunk(0, 10, 100, &chunk)?;
        Ok(())
    }

    #[test]
    fn should_not_find_nonexisting_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let store = super::SqliteChunkStore::new_memory()?;
        let res = store.get_chunk(0, 10, 100)?;
        if (res.is_some()) {
            return Err("there should be no chunk")?;
        }
        Ok(())
    }

    #[test]
    fn should_retrive_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = super::SqliteChunkStore::new_memory()?;
        let chunk = Chunk {
            compressed_data: vec![],
        };
        store.set_chunk(0, 10, 100, &chunk)?;
        let (_, stored) = store.get_chunk(0, 10, 100)?.ok_or("no chunk found")?;
        if (chunk.compressed_data != stored.compressed_data) {
            return Err("chunks don't match")?;
        }
        Ok(())
    }

    #[test]
    fn should_retrive_smallest_chunk() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = super::SqliteChunkStore::new_memory()?;

        store.set_chunk(
            0,
            1,
            9,
            &Chunk {
                compressed_data: vec![2],
            },
        )?;
        store.set_chunk(
            0,
            0,
            50,
            &Chunk {
                compressed_data: vec![5, 6],
            },
        )?;
        store.set_chunk(
            0,
            50,
            200,
            &Chunk {
                compressed_data: vec![5, 6],
            },
        )?;
        store.set_chunk(
            0,
            10,
            100,
            &Chunk {
                compressed_data: vec![],
            },
        )?;
        store.set_chunk(
            0,
            0,
            1000,
            &Chunk {
                compressed_data: vec![1],
            },
        )?;
        let (_, stored) = store.get_chunk(0, 10, 100)?.ok_or("no chunk found")?;
        if stored.compressed_data != vec![] {
            return Err("chunks don't match")?;
        }
        Ok(())
    }

    fn decompressed_eq_compressed(raw: &RawSeries) -> Result<bool, Box<dyn std::error::Error>> {
        let compressed = raw_compress(raw);
        let decompressed = match raw_decompress(&compressed) {
            Ok(v) => v,
            Err(_e) => return Err("failed to decompress")?,
        };

        println!(
            "raw: {}, compressed: {}, ratio: {}",
            raw.serial_size_hint(),
            compressed.len(),
            compressed.len() as f64 / raw.serial_size_hint() as f64
        );
        Ok(raw.eq(&decompressed))
    }

    #[test]
    fn should_cycle_compression() -> Result<(), Box<dyn std::error::Error>> {
        use rand::prelude::*;
        use rand::rngs::SmallRng;
        let mut rng = SmallRng::seed_from_u64(0xdeadbeef);
        let mut series = vec![];
        // float series
        for _ in 0..20 {
            let mut serie: RawSeries = RawSeries::new();
            let num_points: u16 = rng.gen_range(3500..3700);
            let mut last_time_ms: i64 = 1722180250000;
            let mut last_val: f64 = rng.gen_range(0.0..100.0);
            let val_variance: f64 = rng.gen_range(0.01..20.0);
            for _ in 0..num_points {
                let time_inc = rng.gen_range(400..1100);
                last_time_ms += time_inc;
                let time = last_time_ms;
                let val_inc = rng.gen_range(-val_variance..val_variance);
                last_val += val_inc;
                // truncate precision of floats to simulate less random measurements
                let value = f64::from_bits(last_val.to_bits() & (!0 << 36));
                assert!((last_val - value).abs() < 1.0);
                serie.insert(DataPoint { time, value })
            }
            series.push(serie);
        }

        // int as float series
        for _ in 0..30 {
            let mut serie: RawSeries = RawSeries::new();
            let num_points: u16 = rng.gen_range(3500..3700);
            let mut last_time_ms: i64 = 1722180250000;
            let mut last_val: i64 = rng.gen_range(0..100000);
            let val_variance: i64 = rng.gen_range(10..100);
            for _ in 0..num_points {
                let time_inc = rng.gen_range(400..1102);
                last_time_ms += time_inc;
                let time = last_time_ms;
                let val_inc = rng.gen_range(-val_variance..val_variance);
                last_val += val_inc;
                serie.insert(DataPoint {
                    time,
                    value: last_val as f64,
                })
            }
            series.push(serie);
        }

        for serie in series {
            if !decompressed_eq_compressed(&serie)? {
                Err("not equal")?;
            }
        }

        Ok(())
    }

    #[test]
    fn should_compress_raw_series() -> Result<(), Box<dyn std::error::Error>> {
        let mut series = RawSeries::new();
        let test_time_secs = 1722180250;
        for i in 0..3600 * 6 {
            series.insert(DataPoint {
                time: (test_time_secs + i) * 1000,
                value: i as f64 * 100.0,
            });
        }
        let encoded = raw_compress(&series);
        println!(
            "raw: {}, compressed: {}, ratio: {}",
            series.serial_size_hint(),
            encoded.len(),
            encoded.len() as f64 / series.serial_size_hint() as f64
        );
        Ok(())
    }
}
