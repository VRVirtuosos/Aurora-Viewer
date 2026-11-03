//! Adversarial Stress & Correctness Tests for `aurora-leap` framing codec.
//!
//! Validates:
//! 1. Chunked fragmentation (byte-by-byte feeds, arbitrary slice sizes).
//! 2. Rapid pipelining (10,000 continuous frames without delimiters).
//! 3. Boundary conditions (empty payloads, nested maps, wide maps, 1MB–16MB payloads).
//! 4. Adversarial attacks (non-numeric prefixes, negative lengths, length overflow, colon-less streams, corrupted binary bytes).

use std::time::Instant;

use aurora_leap::codec::{
    CodecError, LeapDecoder, LeapEncoder, LeapMessage, DEFAULT_MAX_PAYLOAD_SIZE, MAX_PREFIX_LEN,
};
use aurora_llsd::{llsd_map, Llsd, Map};

// ============================================================================
// 1. Chunked Fragmentation Tests
// ============================================================================

#[test]
fn test_adversarial_byte_by_byte_stream() {
    let mut decoder = LeapDecoder::default();

    // Create 5 distinct messages with diverse contents
    let messages = vec![
        LeapMessage::new("pump_alpha", llsd_map! { "seq" => 1, "text" => "alpha message" }),
        LeapMessage::new("pump_beta", llsd_map! { "seq" => 2, "enabled" => true, "ratio" => 3.14159 }),
        LeapMessage::new("pump_gamma", llsd_map! { "seq" => 3, "nested" => llsd_map! { "inner" => "val" } }),
        LeapMessage::new("pump_delta", llsd_map! { "seq" => 4, "binary_data" => vec![0u8, 1, 2, 3, 255] }),
        LeapMessage::new("pump_omega", llsd_map! { "seq" => 5, "empty" => "" }),
    ];

    let mut full_stream = Vec::new();
    for msg in &messages {
        full_stream.extend_from_slice(&LeapEncoder::encode(msg));
    }

    let mut decoded_messages = Vec::new();

    // Feed one single byte at a time
    for (idx, &byte) in full_stream.iter().enumerate() {
        decoder.push_bytes(&[byte]);
        loop {
            match decoder.decode_next() {
                Ok(Some(msg)) => decoded_messages.push(msg),
                Ok(None) => break,
                Err(err) => panic!("unexpected error at byte index {idx}: {err:?}"),
            }
        }
    }

    assert_eq!(decoded_messages.len(), 5);
    for i in 0..5 {
        assert_eq!(decoded_messages[i].pump, messages[i].pump);
        assert_eq!(decoded_messages[i].data.get("seq").as_i32(), (i + 1) as i32);
    }
    assert!(!decoder.has_unconsumed_data());
}

#[test]
fn test_adversarial_prime_sized_chunks() {
    let mut decoder = LeapDecoder::default();

    // Generate 50 messages
    let mut full_stream = Vec::new();
    for i in 0..50 {
        let msg = LeapMessage::new(
            format!("pump_{i}"),
            llsd_map! {
                "index" => i as i32,
                "padding" => "x".repeat((i * 17) % 200)
            },
        );
        full_stream.extend_from_slice(&LeapEncoder::encode(&msg));
    }

    // Feed across prime chunk sizes (1, 3, 7, 13, 29, 61, 127)
    let prime_chunks = [1, 3, 7, 13, 29, 61, 127];
    let mut chunk_idx = 0;
    let mut offset = 0;
    let mut decoded_count = 0;

    while offset < full_stream.len() {
        let chunk_size = prime_chunks[chunk_idx % prime_chunks.len()];
        let end = (offset + chunk_size).min(full_stream.len());
        decoder.push_bytes(&full_stream[offset..end]);
        offset = end;
        chunk_idx += 1;

        while let Some(msg) = decoder.decode_next().expect("decode failure on prime chunks") {
            assert_eq!(msg.pump, format!("pump_{decoded_count}"));
            assert_eq!(msg.data.get("index").as_i32(), decoded_count as i32);
            decoded_count += 1;
        }
    }

    assert_eq!(decoded_count, 50);
    assert!(!decoder.has_unconsumed_data());
}

// ============================================================================
// 2. Rapid Pipelining Stress Test (10,000 Frames)
// ============================================================================

#[test]
fn test_adversarial_pipelining_10000_frames() {
    let count = 10_000;
    let mut stream = Vec::with_capacity(count * 64);

    let start_encode = Instant::now();
    for i in 0..count {
        let msg = LeapMessage::new("pipe_pump", llsd_map! { "seq" => i as i32 });
        stream.extend_from_slice(&LeapEncoder::encode(&msg));
    }
    let encode_duration = start_encode.elapsed();
    println!("Encoded {count} frames in {encode_duration:?} ({:.2} MB)", stream.len() as f64 / (1024.0 * 1024.0));

    // Decode from single contiguous buffer
    let mut decoder = LeapDecoder::default();
    let start_decode = Instant::now();

    // Push all 10,000 frames into decoder accumulator at once
    decoder.push_bytes(&stream);

    let mut decoded_count = 0;
    while let Some(msg) = decoder.decode_next().expect("pipelining decode failed") {
        assert_eq!(msg.pump, "pipe_pump");
        assert_eq!(msg.data.get("seq").as_i32(), decoded_count);
        decoded_count += 1;
    }

    let decode_duration = start_decode.elapsed();
    println!("Decoded {count} frames in {decode_duration:?}");

    assert_eq!(decoded_count, count as i32);
    assert!(!decoder.has_unconsumed_data());

    // Sanity performance check: decoding 10,000 frames must finish in reasonable time (< 5 seconds)
    assert!(decode_duration.as_secs() < 5, "pipelining 10k frames took too long: {decode_duration:?}");
}

// ============================================================================
// 3. Boundary & Scale Stress Tests
// ============================================================================

#[test]
fn test_boundary_empty_payloads() {
    let mut decoder = LeapDecoder::default();

    // 1. Empty string in data field
    let msg1 = LeapMessage::new("empty_str_pump", llsd_map! { "val" => "" });
    let frame1 = LeapEncoder::encode(&msg1);
    decoder.push_bytes(&frame1);
    let dec1 = decoder.decode_next().unwrap().unwrap();
    assert_eq!(dec1.pump, "empty_str_pump");
    assert_eq!(dec1.data.get("val").as_str(), "");

    // 2. Empty map in data field
    let msg2 = LeapMessage::new("empty_map_pump", llsd_map! {});
    let frame2 = LeapEncoder::encode(&msg2);
    decoder.push_bytes(&frame2);
    let dec2 = decoder.decode_next().unwrap().unwrap();
    assert_eq!(dec2.pump, "empty_map_pump");
    assert_eq!(dec2.data, Llsd::Map(Map::new()));

    // 3. Top-level zero-length payload `0:` (invalid envelope because 0 bytes is not LLSD)
    decoder.push_bytes(b"0:");
    match decoder.decode_next() {
        Err(CodecError::Llsd(_)) => {}
        other => panic!("expected Llsd error for empty 0: payload, got: {other:?}"),
    }
}

#[test]
fn test_boundary_deeply_nested_maps() {
    let mut decoder = LeapDecoder::default();

    // Build 60 levels of nested maps
    let mut current = llsd_map! { "bottom" => "reached" };
    for level in (0..60).rev() {
        current = llsd_map! { format!("level_{level}") => current };
    }

    let msg = LeapMessage::new("deep_nesting_pump", current);
    let encoded = LeapEncoder::encode(&msg);
    decoder.push_bytes(&encoded);

    let decoded = decoder.decode_next().unwrap().unwrap();
    assert_eq!(decoded.pump, "deep_nesting_pump");

    // Traverse down to bottom
    let mut curr_val = &decoded.data;
    for level in 0..60 {
        curr_val = curr_val.get(&format!("level_{level}"));
        assert!(curr_val.is_map());
    }
    assert_eq!(curr_val.get("bottom").as_str(), "reached");
}

#[test]
fn test_boundary_wide_map() {
    let mut decoder = LeapDecoder::default();

    // Map with 2,000 keys
    let mut map = Map::new();
    for i in 0..2000 {
        map.insert(format!("key_{i:04}"), Llsd::Integer(i));
    }

    let msg = LeapMessage::new("wide_map_pump", Llsd::Map(map));
    let encoded = LeapEncoder::encode(&msg);
    decoder.push_bytes(&encoded);

    let decoded = decoder.decode_next().unwrap().unwrap();
    assert_eq!(decoded.pump, "wide_map_pump");
    assert_eq!(decoded.data.get("key_0000").as_i32(), 0);
    assert_eq!(decoded.data.get("key_1999").as_i32(), 1999);
}

#[test]
fn test_boundary_large_payload_1mb() {
    let mut decoder = LeapDecoder::default();

    // 1 MB payload string
    let large_str = "A".repeat(1024 * 1024);
    let msg = LeapMessage::new("large_pump", llsd_map! { "content" => large_str.as_str() });
    let encoded = LeapEncoder::encode(&msg);

    assert!(encoded.len() > 1024 * 1024);

    let start = Instant::now();
    decoder.push_bytes(&encoded);
    let decoded = decoder.decode_next().unwrap().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(decoded.pump, "large_pump");
    assert_eq!(decoded.data.get("content").as_str().len(), 1024 * 1024);
    println!("1 MB payload decoded in {elapsed:?}");
}

#[test]
fn test_boundary_large_payload_8mb() {
    let mut decoder = LeapDecoder::default();

    // 8 MB payload string
    let large_str = "B".repeat(8 * 1024 * 1024);
    let msg = LeapMessage::new("large_8mb", llsd_map! { "content" => large_str.as_str() });
    let encoded = LeapEncoder::encode(&msg);

    assert!(encoded.len() > 8 * 1024 * 1024);

    let start = Instant::now();
    decoder.push_bytes(&encoded);
    let decoded = decoder.decode_next().unwrap().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(decoded.pump, "large_8mb");
    assert_eq!(decoded.data.get("content").as_str().len(), 8 * 1024 * 1024);
    println!("8 MB payload decoded in {elapsed:?}");
}

#[test]
fn test_boundary_exact_max_payload_size_16mb() {
    // Custom decoder with 4 MB limit to test exact boundary without huge allocations
    let limit = 4 * 1024 * 1024;
    let mut decoder = LeapDecoder::new(limit, MAX_PREFIX_LEN);

    // Construct raw notation frame with exact length == limit
    // Prefix: "<limit>:" + exact payload of length <limit>
    let raw_payload = format!("{{'pump':'exact','data':'{}'}}", "X".repeat(limit - 32));
    let actual_len = raw_payload.len();

    // Test frame within limit
    let frame_within = format!("{actual_len}:{raw_payload}");
    decoder.push_bytes(frame_within.as_bytes());
    let dec = decoder.decode_next().unwrap().unwrap();
    assert_eq!(dec.pump, "exact");

    // Now test frame that exceeds limit by 1 byte
    let oversized_prefix = format!("{}:{}", limit + 1, "Y".repeat(10));
    decoder.push_bytes(oversized_prefix.as_bytes());
    match decoder.decode_next() {
        Err(CodecError::PayloadTooLarge { length, max }) => {
            assert_eq!(length, limit + 1);
            assert_eq!(max, limit);
        }
        other => panic!("expected PayloadTooLarge, got: {other:?}"),
    }
}

// ============================================================================
// 4. Adversarial Rejection Tests
// ============================================================================

#[test]
fn test_adversarial_non_numeric_prefixes() {
    let test_cases = vec![
        "abc:{'pump':'a','data':1}",
        "12a4:{'pump':'a','data':1}",
        "-5:{'pump':'a','data':1}",
        "+10:{'pump':'a','data':1}",
        " 10:{'pump':'a','data':1}",
        "10 :{'pump':'a','data':1}",
        "0x10:{'pump':'a','data':1}",
        "1e5:{'pump':'a','data':1}",
        "1.5:{'pump':'a','data':1}",
        "\0:{'pump':'a','data':1}",
    ];

    for case in test_cases {
        let mut decoder = LeapDecoder::default();
        decoder.push_bytes(case.as_bytes());
        match decoder.decode_next() {
            Err(CodecError::NonNumericPrefix(_)) => {}
            other => panic!("case '{case}' should produce NonNumericPrefix, got: {other:?}"),
        }
    }
}

#[test]
fn test_adversarial_negative_and_overflow_lengths() {
    // Negative length
    let mut decoder1 = LeapDecoder::default();
    decoder1.push_bytes(b"-99999999999:{'pump':'a','data':1}");
    match decoder1.decode_next() {
        Err(CodecError::NonNumericPrefix(_)) => {}
        other => panic!("expected NonNumericPrefix for negative length, got: {other:?}"),
    }

    // Overflow u64::MAX (20 digits of 9 is > u64::MAX)
    let mut decoder2 = LeapDecoder::default();
    decoder2.push_bytes(b"99999999999999999999:{}");
    match decoder2.decode_next() {
        Err(CodecError::InvalidLengthPrefix(_)) => {}
        other => panic!("expected InvalidLengthPrefix for u64 overflow, got: {other:?}"),
    }
}

#[test]
fn test_adversarial_prefix_length_bounds() {
    // Prefix exceeding MAX_PREFIX_LEN (20 bytes) with colon
    let mut decoder = LeapDecoder::new(DEFAULT_MAX_PAYLOAD_SIZE, 20);
    decoder.push_bytes(b"123456789012345678901:{}");
    match decoder.decode_next() {
        Err(CodecError::PrefixTooLong { max: 20 }) => {}
        other => panic!("expected PrefixTooLong, got: {other:?}"),
    }

    // Prefix exceeding MAX_PREFIX_LEN without colon (streaming digits)
    let mut decoder2 = LeapDecoder::new(DEFAULT_MAX_PAYLOAD_SIZE, 20);
    decoder2.push_bytes(b"123456789012345678901");
    match decoder2.decode_next() {
        Err(CodecError::PrefixTooLong { max: 20 }) => {}
        other => panic!("expected PrefixTooLong, got: {other:?}"),
    }
}

#[test]
fn test_adversarial_colon_less_streams() {
    // 1. Raw text without colon (e.g. terminal log)
    let mut decoder1 = LeapDecoder::default();
    decoder1.push_bytes(b"print(\"hello viewer\")\r\n");
    match decoder1.decode_next() {
        Err(CodecError::UnexpectedPrefixChar('\r')) => {}
        other => panic!("expected UnexpectedPrefixChar, got: {other:?}"),
    }

    // 2. Binary noise without colon or digits
    let mut decoder2 = LeapDecoder::default();
    decoder2.push_bytes(&[0xFF, 0xFE, 0x00, 0x01, 0x80]);
    match decoder2.decode_next() {
        Err(CodecError::NonNumericPrefix(_)) => {}
        other => panic!("expected NonNumericPrefix for binary noise, got: {other:?}"),
    }
}

#[test]
fn test_adversarial_corrupted_payload_bytes() {
    // 1. Valid prefix (10 bytes), but payload is malformed notation
    let mut decoder1 = LeapDecoder::default();
    decoder1.push_bytes(b"10:{'pump':??}");
    match decoder1.decode_next() {
        Err(CodecError::Llsd(_)) => {}
        other => panic!("expected Llsd parse error for corrupted notation, got: {other:?}"),
    }

    // 2. Truncated binary header
    let mut decoder2 = LeapDecoder::default();
    decoder2.push_bytes(b"12:<? LLSD/Bina");
    match decoder2.decode_next() {
        Err(CodecError::Llsd(_)) => {}
        other => panic!("expected Llsd error for truncated binary header, got: {other:?}"),
    }

    // 3. Binary header followed by random corrupted bytes
    let mut decoder3 = LeapDecoder::default();
    let mut corrupted_bin = b"<? LLSD/Binary ?>\n".to_vec();
    corrupted_bin.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x7F]);
    let frame = format!("{}:", corrupted_bin.len());
    let mut full = frame.into_bytes();
    full.extend_from_slice(&corrupted_bin);

    decoder3.push_bytes(&full);
    match decoder3.decode_next() {
        Err(CodecError::Llsd(_)) => {}
        other => panic!("expected Llsd error for corrupted binary bytes, got: {other:?}"),
    }
}

#[test]
fn test_adversarial_malformed_envelope_variants() {
    let test_cases = vec![
        // Valid notation integer, but not map
        "i42",
        // Valid notation array, not map
        "[i1, i2]",
        // Map missing 'pump'
        "{'data':'test'}",
        // Map missing 'data'
        "{'pump':'test'}",
        // Map where 'pump' is integer instead of string
        "{'pump':i42,'data':i1}",
        // Map where 'pump' is boolean (1 in notation)
        "{'pump':1,'data':'val'}",
    ];

    for raw in test_cases {
        let frame = format!("{}:{}", raw.len(), raw);
        let mut decoder = LeapDecoder::default();
        decoder.push_bytes(frame.as_bytes());
        match decoder.decode_next() {
            Err(CodecError::InvalidEnvelope(_)) => {}
            other => panic!("case '{frame}' should fail with InvalidEnvelope, got: {other:?}"),
        }
    }
}
