use std::collections::BTreeMap;

use pco::standalone::{simple_decompress, simpler_compress};
use pco::DEFAULT_COMPRESSION_LEVEL;

#[cfg(test)]
use quickcheck::Arbitrary;

#[derive(Clone, Copy, Debug)]
pub struct DataPoint {
    pub time: i64,
    pub value: f64,
}

#[cfg(test)]
impl Arbitrary for DataPoint {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        DataPoint {
            time: u32::arbitrary(g) as i64,
            value: f32::arbitrary(g) as f64,
        }
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let time = self.time;
        let value = self.value;
        let iter = i64::shrink(&time)
            .zip(f64::shrink(&value))
            .map(|(time, value)| DataPoint { time, value });
        Box::new(iter)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct RawSeries {
    pub(crate) data: BTreeMap<i64, f64>,
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

    pub fn first_time(&self) -> Option<i64> {
        Some(*self.data.first_key_value()?.0)
    }

    pub fn last_time(&self) -> Option<i64> {
        Some(*self.data.last_key_value()?.0)
    }
}

#[derive(Debug)]
pub enum DecompressError {
    TimeHeaderMissing,
    TimesMissing,
    ValHeaderMissing,
    ValsMissing,
    DecompressError(Box<dyn std::error::Error + 'static>),
}

impl From<Box<dyn std::error::Error + 'static>> for DecompressError {
    fn from(value: Box<dyn std::error::Error + 'static>) -> Self {
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

    let times = simple_decompress::<i64>(compressed_times).unwrap();
    let values = simple_decompress::<f64>(compressed_vals).unwrap();

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
    let compressed_times = {
        let timevec: Vec<i64> = raw.data.keys().copied().collect();
        simpler_compress(&timevec, DEFAULT_COMPRESSION_LEVEL).unwrap()
    };
    let compressed_vals = {
        let valvec: Vec<f64> = raw.data.values().copied().collect();
        simpler_compress(&valvec, DEFAULT_COMPRESSION_LEVEL).unwrap()
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
        Chunk {
            compressed_data: raw_compress(series),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{raw_compress, raw_decompress, DataPoint, RawSeries};
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
        for _ in 0..500 {
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
                let value = f64::from_bits(last_val.to_bits() & (!0 << 16));
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
