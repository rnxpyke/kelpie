#![no_main]

use libfuzzer_sys::fuzz_target;

use pco::standalone::{simpler_compress, simple_decompress};
use pco::DEFAULT_COMPRESSION_LEVEL;
use pco::errors::PcoResult;

fuzz_target!(|data: Vec<f64>| {
    let mut data = data.clone();
    data.retain(|x| !x.is_nan());
    let compressed = simpler_compress(&data, DEFAULT_COMPRESSION_LEVEL).unwrap();
    let recovered = simple_decompress::<f64>(&compressed).unwrap();
    assert_eq!(data, recovered);
    // fuzzed code goes here
});
