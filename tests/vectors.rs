//! The test vectors of the specification (`rope/docs/vectors/frames.json`, spec 14.6): every valid frame decodes to
//! the given meaning and, where marked exact, encodes to the same bytes; every invalid one fails with its code.

use rope_protocol::frames::{self, Decoder, Frame};
use serde_json::Value;
use std::path::PathBuf;

fn vectors() -> Value {
    // the Python reference is the source of the vectors; a copy lives in this repository
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("vectors")
        .join("frames.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn valid_vectors_decode_and_encode() {
    let v = vectors();
    let valid = v["valid"].as_array().unwrap();
    assert!(valid.len() >= 16);
    for vec in valid {
        let name = vec["name"].as_str().unwrap();
        let raw = hex(vec["frame_hex"].as_str().unwrap());
        let f = frames::decode(&raw).unwrap_or_else(|e| panic!("{}: {}", name, e));
        frames::validate(&f).unwrap_or_else(|e| panic!("{}: {}", name, e));
        assert_eq!(f.ftype as u64, vec["type"].as_u64().unwrap(), "{}", name);
        assert_eq!(Value::Object(f.meta.clone()), vec["meta"], "{}", name);
        assert_eq!(f.blob, hex(vec["blob_hex"].as_str().unwrap()), "{}", name);
        if let Some(raw_data) = vec.get("raw_data_hex").and_then(Value::as_str) {
            assert_eq!(frames::payload(&f).unwrap(), hex(raw_data), "{}", name);
        }
        if vec["encode_exact"].as_bool().unwrap() {
            let again = Frame::with_blob(f.ftype, f.meta.clone(), f.blob.clone());
            assert_eq!(
                frames::encode(&again).unwrap(),
                raw,
                "{}: encoding differs",
                name
            );
        }
        // fed byte by byte, the incremental decoder gives the same frame
        let mut d = Decoder::new();
        let mut got = Vec::new();
        for b in &raw {
            got.extend(d.feed(&[*b]).unwrap());
        }
        assert_eq!(got, vec![f], "{}", name);
    }
}

#[test]
fn invalid_vectors_fail_with_their_code() {
    let v = vectors();
    for vec in v["invalid"].as_array().unwrap() {
        let name = vec["name"].as_str().unwrap();
        let raw = hex(vec["frame_hex"].as_str().unwrap());
        let err = Decoder::new()
            .feed(&raw)
            .and_then(|fs| fs.iter().try_for_each(frames::validate).map(|_| fs))
            .expect_err(name);
        assert_eq!(
            err.code,
            vec["error"].as_str().unwrap(),
            "{}: {}",
            name,
            err.text
        );
    }
}

#[test]
fn zlib_payload_is_bounded() {
    let data = vec![7u8; 100_000];
    let (enc, blob) = frames::encode_payload(&data, 1);
    assert_eq!(enc, "zlib");
    assert_eq!(frames::decode_payload(enc, &blob, 100_000).unwrap(), data);
    assert_eq!(
        frames::decode_payload(enc, &blob, 10).unwrap_err().code,
        "bad_frame"
    );
    let mut extra = blob.clone();
    extra.push(0);
    assert!(frames::decode_payload(enc, &extra, 100_000).is_err());
}
