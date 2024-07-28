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
            .with_delta_encoding_order(2);
        let timevec: Vec<i64> = raw.data.keys().copied().collect();

        q_compress::Compressor::from_config(cfg).simple_compress(&timevec)
    };
    let compressed_vals = {
        let cfg = CompressorConfig::default()
            .with_use_gcds(false)
            .with_delta_encoding_order(1);
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

pub struct Chunk {}

pub enum SetChunkError {}

pub trait KelpieChunkStore {
    fn get_chunk(series_key: u64, start: u64, stop: u64) -> Option<Chunk>;
    fn set_chunk(series_key: u64, start: u64, stop: u64, chunk: Chunk)
        -> Result<(), SetChunkError>;
}

pub struct SqliteChunkStore {
    db: sqlite::Connection,
}

impl SqliteChunkStore {
    fn migrate(db: &mut sqlite::Connection) -> Result<(), sqlite::Error> {
        db.execute("CREATE TABLE IF NOT EXISTS chunks (series INTEGER, start INTEGER, stop INTEGER, chunk BLOB)")?;
        Ok(())
    }

    fn new_memory() -> Result<Self, sqlite::Error> {
        let mut db = sqlite::open(":memory:")?;
        Self::migrate(&mut db)?;
        Ok(Self { db })
    }
}

impl KelpieChunkStore for SqliteChunkStore {
    fn get_chunk(series_key: u64, start: u64, stop: u64) -> Option<Chunk> {
        todo!()
    }

    fn set_chunk(
        series_key: u64,
        start: u64,
        stop: u64,
        chunk: Chunk,
    ) -> Result<(), SetChunkError> {
        todo!()
    }
}

pub struct KelpieSeries {
    start_timestamp: u64,
}

#[cfg(test)]
mod tests {
    use crate::{raw_compress, raw_decompress, DataPoint, RawSeries};

    #[test]
    fn should_create_sqlite_chunk_store() -> Result<(), Box<dyn std::error::Error>> {
        let _store = super::SqliteChunkStore::new_memory()?;
        Ok(())
    }

    fn decompressed_eq_compressed(raw: &RawSeries) -> Result<bool, Box<dyn std::error::Error>> {
        let compressed = raw_compress(raw);
        let decompressed = match raw_decompress(&compressed) {
            Ok(v) => v,
            Err(e) => return Err("failed to decompress")?,
        };

        Ok(raw.eq(&decompressed))
    }

    #[test]
    fn should_cycle_compression() -> Result<(), Box<dyn std::error::Error>> {
        let mut series = RawSeries::new();
        let test_time_secs = 1722180250;
        for i in 0..3600 * 6 {
            series.insert(DataPoint {
                time: (test_time_secs + i) * 1000,
                value: i as f64 * 100.0,
            });
        }
        if !decompressed_eq_compressed(&series)? {
            Err("not equal")?;
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
        print!(
            "raw: {}, compressed: {}, ratio: {}",
            series.serial_size_hint(),
            encoded.len(),
            encoded.len() as f64 / series.serial_size_hint() as f64
        );
        Ok(())
    }
}
