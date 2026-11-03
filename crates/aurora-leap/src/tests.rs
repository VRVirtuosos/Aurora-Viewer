//! Comprehensive unit tests for `aurora-leap`.
//!
//! Covers:
//! - Codec: round-trip, chunked partial reads, pipelining, malformed/non-numeric prefix rejection, size limits, binary auto-detection.
//! - EventPumps: pub/sub distribution, maildrop queueing and flushing, duplicate rejection, deadlock freedom.
//! - Command: handshake intro generation, ping/pong, reqid correlation, getAPIs metadata, error wrapping.
//! - Transport Loopback: in-memory duplex MockPipe verifying end-to-end communication without external processes.

use std::collections::VecDeque;
use std::io::{Read, Result as IoResult, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use aurora_llsd::{llsd_map, to_binary, Llsd};
use uuid::Uuid;

use crate::codec::{CodecError, LeapDecoder, LeapEncoder, LeapMessage};
use crate::command::{ApiMetadata, LeapCommandHandler, LeapSession, OperationMetadata};
use crate::pump::{EventPumps, PumpError, PumpType};
use crate::transport::parse_command_line;
use crate::LeapManager;

// ============================================================================
// 1. Codec Unit Tests
// ============================================================================

#[test]
fn test_codec_roundtrip() {
    let mut decoder = LeapDecoder::default();
    let original = LeapMessage::new(
        "reply_pump",
        llsd_map! {
            "status" => true,
            "reqid" => 101,
            "greeting" => "Hello LEAP!"
        },
    );

    let encoded = LeapEncoder::encode(&original);
    decoder.push_bytes(&encoded);

    let decoded = decoder
        .decode_next()
        .expect("decoding failed")
        .expect("expected a message");

    assert_eq!(decoded.pump, "reply_pump");
    assert_eq!(decoded.data.get("status").as_bool(), true);
    assert_eq!(decoded.data.get("reqid").as_i32(), 101);
    assert_eq!(decoded.data.get("greeting").as_str(), "Hello LEAP!");
    assert_eq!(decoder.decode_next().unwrap(), None);
}

#[test]
fn test_codec_chunked_reads() {
    let mut decoder = LeapDecoder::default();
    let original = LeapMessage::new(
        "streaming_pump",
        llsd_map! { "counter" => 42, "data" => "chunked delivery test" },
    );

    let encoded = LeapEncoder::encode(&original);

    // Feed bytes one at a time to test stateful accumulation
    for (i, &byte) in encoded.iter().enumerate() {
        decoder.push_bytes(&[byte]);
        let result = decoder.decode_next().expect("decode error on chunk");
        if i + 1 < encoded.len() {
            assert!(
                result.is_none(),
                "should return None before complete frame is assembled (at byte {i})"
            );
        } else {
            let msg = result.expect("expected complete message at final byte");
            assert_eq!(msg.pump, "streaming_pump");
            assert_eq!(msg.data.get("counter").as_i32(), 42);
        }
    }
}

#[test]
fn test_codec_multipacket_pipelining() {
    let mut decoder = LeapDecoder::default();

    let msg1 = LeapMessage::new("pump1", llsd_map! { "id" => 1 });
    let msg2 = LeapMessage::new("pump2", llsd_map! { "id" => 2 });
    let msg3 = LeapMessage::new("pump3", llsd_map! { "id" => 3 });

    let mut stream = Vec::new();
    stream.extend_from_slice(&LeapEncoder::encode(&msg1));
    stream.extend_from_slice(&LeapEncoder::encode(&msg2));
    stream.extend_from_slice(&LeapEncoder::encode(&msg3));

    decoder.push_bytes(&stream);

    let res1 = decoder.decode_next().unwrap().unwrap();
    assert_eq!(res1.pump, "pump1");
    assert_eq!(res1.data.get("id").as_i32(), 1);

    let res2 = decoder.decode_next().unwrap().unwrap();
    assert_eq!(res2.pump, "pump2");
    assert_eq!(res2.data.get("id").as_i32(), 2);

    let res3 = decoder.decode_next().unwrap().unwrap();
    assert_eq!(res3.pump, "pump3");
    assert_eq!(res3.data.get("id").as_i32(), 3);

    assert_eq!(decoder.decode_next().unwrap(), None);
}

#[test]
fn test_codec_non_numeric_prefix() {
    let mut decoder = LeapDecoder::default();
    decoder.push_bytes(b"5a2:{'pump':'test','data':1}");

    match decoder.decode_next() {
        Err(CodecError::NonNumericPrefix(_)) => {}
        other => panic!("expected NonNumericPrefix, got: {other:?}"),
    }
}

#[test]
fn test_codec_unexpected_prefix_char_newline() {
    let mut decoder = LeapDecoder::default();
    decoder.push_bytes(b"print(\"hello\")\n");

    match decoder.decode_next() {
        Err(CodecError::UnexpectedPrefixChar('\n')) => {}
        other => panic!("expected UnexpectedPrefixChar, got: {other:?}"),
    }
}

#[test]
fn test_codec_empty_prefix() {
    let mut decoder = LeapDecoder::default();
    decoder.push_bytes(b":{'pump':'test','data':1}");

    match decoder.decode_next() {
        Err(CodecError::InvalidLengthPrefix(_)) => {}
        other => panic!("expected InvalidLengthPrefix, got: {other:?}"),
    }
}

#[test]
fn test_codec_prefix_too_long() {
    let mut decoder = LeapDecoder::new(1024, 10);
    decoder.push_bytes(b"1234567890123");

    match decoder.decode_next() {
        Err(CodecError::PrefixTooLong { max: 10 }) => {}
        other => panic!("expected PrefixTooLong, got: {other:?}"),
    }
}

#[test]
fn test_codec_payload_too_large() {
    let mut decoder = LeapDecoder::new(50, 20);
    // Payload length prefix is 100, but limit is 50
    decoder.push_bytes(b"100:{'data':1}");

    match decoder.decode_next() {
        Err(CodecError::PayloadTooLarge { length: 100, max: 50 }) => {}
        other => panic!("expected PayloadTooLarge, got: {other:?}"),
    }
}

#[test]
fn test_codec_invalid_envelope() {
    let mut decoder = LeapDecoder::default();
    // Valid LLSD integer, but not an envelope map with 'pump' and 'data'
    decoder.push_bytes(b"3:i42");

    match decoder.decode_next() {
        Err(CodecError::InvalidEnvelope(_)) => {}
        other => panic!("expected InvalidEnvelope, got: {other:?}"),
    }

    // Valid map, but missing 'data'
    let mut decoder2 = LeapDecoder::default();
    decoder2.push_bytes(b"15:{'pump':'test'}");
    match decoder2.decode_next() {
        Err(CodecError::InvalidEnvelope(_)) => {}
        other => panic!("expected InvalidEnvelope, got: {other:?}"),
    }
}

#[test]
fn test_codec_binary_llsd_auto_detect() {
    let mut decoder = LeapDecoder::default();

    let msg = LeapMessage::new("binary_pump", llsd_map! { "binary_val" => 12345 });
    let mut binary_payload = b"<? LLSD/Binary ?>\n".to_vec();
    binary_payload.extend_from_slice(&to_binary(&msg.to_llsd()));

    let len_str = binary_payload.len().to_string();
    let mut framed = Vec::new();
    framed.extend_from_slice(len_str.as_bytes());
    framed.push(b':');
    framed.extend_from_slice(&binary_payload);

    decoder.push_bytes(&framed);
    let decoded = decoder.decode_next().unwrap().unwrap();
    assert_eq!(decoded.pump, "binary_pump");
    assert_eq!(decoded.data.get("binary_val").as_i32(), 12345);
}

// ============================================================================
// 2. EventPumps Unit Tests
// ============================================================================

#[test]
fn test_event_pumps_pubsub_multiple_listeners() {
    let pumps = EventPumps::new();
    let pump = pumps.obtain("ChatPump");

    let received_a = Arc::new(Mutex::new(Vec::new()));
    let received_b = Arc::new(Mutex::new(Vec::new()));

    let rec_a = received_a.clone();
    assert!(pump.listen("listener_a", move |ev| {
        rec_a.lock().push(ev.clone());
        false
    }));

    let rec_b = received_b.clone();
    assert!(pump.listen("listener_b", move |ev| {
        rec_b.lock().push(ev.clone());
        false
    }));

    assert_eq!(pump.listener_names(), vec!["listener_a", "listener_b"]);

    let event = llsd_map! { "message" => "Hello everyone!" };
    pump.post(&event);

    assert_eq!(received_a.lock().len(), 1);
    assert_eq!(
        received_a.lock()[0].get("message").as_str(),
        "Hello everyone!"
    );
    assert_eq!(received_b.lock().len(), 1);
    assert_eq!(
        received_b.lock()[0].get("message").as_str(),
        "Hello everyone!"
    );
}

#[test]
fn test_event_pumps_consumption_stops_propagation() {
    let pumps = EventPumps::new();
    let pump = pumps.obtain("FilterPump");

    let called_b = Arc::new(AtomicBool::new(false));

    // Listener A consumes the event (returns true)
    pump.listen("listener_a", |_ev| true);

    let b_flag = called_b.clone();
    pump.listen("listener_b", move |_ev| {
        b_flag.store(true, Ordering::SeqCst);
        false
    });

    pump.post(&llsd_map! { "test" => 1 });

    assert_eq!(
        called_b.load(Ordering::SeqCst),
        false,
        "listener_b should not be called because listener_a consumed event"
    );
}

#[test]
fn test_event_pumps_duplicate_listener_rejected() {
    let pumps = EventPumps::new();
    let pump = pumps.obtain("UniquePump");

    assert!(pump.listen("my_sub", |_| false));
    assert_eq!(
        pump.listen("my_sub", |_| false),
        false,
        "duplicate listener name must be rejected"
    );
}

#[test]
fn test_event_pumps_stop_listening() {
    let pumps = EventPumps::new();
    let pump = pumps.obtain("SubPump");

    let call_count = Arc::new(Mutex::new(0));
    let cc = call_count.clone();

    pump.listen("temp_listener", move |_| {
        *cc.lock() += 1;
        false
    });

    pump.post(&llsd_map! { "x" => 1 });
    assert_eq!(*call_count.lock(), 1);

    assert!(pump.stop_listening("temp_listener"));
    assert_eq!(
        pump.stop_listening("temp_listener"),
        false,
        "stopping non-existent listener must return false"
    );

    pump.post(&llsd_map! { "x" => 2 });
    assert_eq!(
        *call_count.lock(),
        1,
        "no callbacks should fire after stop_listening"
    );
}

#[test]
fn test_maildrop_queueing_and_flush() {
    let pumps = EventPumps::new();
    let maildrop = pumps
        .make("PendingMail", false, "LLEventMailDrop")
        .expect("failed to make maildrop");

    assert_eq!(maildrop.pump_type(), PumpType::MailDrop);

    // Post 2 messages while offline
    maildrop.post(&llsd_map! { "seq" => 1 });
    maildrop.post(&llsd_map! { "seq" => 2 });
    assert_eq!(maildrop.history_len(), 2);

    let received = Arc::new(Mutex::new(Vec::new()));
    let rec = received.clone();

    // Attaching listener should flush queued history in FIFO order
    maildrop.listen("receiver", move |ev| {
        rec.lock().push(ev.clone());
        true // consume
    });

    assert_eq!(received.lock().len(), 2);
    assert_eq!(received.lock()[0].get("seq").as_i32(), 1);
    assert_eq!(received.lock()[1].get("seq").as_i32(), 2);
    assert_eq!(maildrop.history_len(), 0);
}

#[test]
fn test_maildrop_discard() {
    let pumps = EventPumps::new();
    let maildrop = pumps.make("DropHistory", false, "LLEventMailDrop").unwrap();

    maildrop.post(&llsd_map! { "item" => "discard_me" });
    assert_eq!(maildrop.history_len(), 1);

    maildrop.discard();
    assert_eq!(maildrop.history_len(), 0);
}

#[test]
fn test_deadlock_free_nested_post() {
    let pumps = EventPumps::new();
    let pump_a = pumps.obtain("PumpA");
    let pumps_c = pumps.clone();

    // When PumpA fires, it posts to PumpB and attaches a new listener to PumpB
    pump_a.listen("nested_tester", move |_ev| {
        let pump_b = pumps_c.obtain("PumpB");
        pump_b.listen("b_sub", |_| false);
        pump_b.post(&llsd_map! { "nested" => true });
        true
    });

    // Should complete cleanly without deadlocking
    assert!(pump_a.post(&llsd_map! { "trigger" => 1 }));
    assert!(pumps.obtain("PumpB").has_listener("b_sub"));
}

#[test]
fn test_pump_registry_make_and_tweak() {
    let pumps = EventPumps::new();

    let p1 = pumps.make("SharedName", false, "LLEventStream").unwrap();
    assert_eq!(p1.name(), "SharedName");

    // Without tweak: duplicate returns error
    match pumps.make("SharedName", false, "LLEventStream") {
        Err(PumpError::DupPumpName(_)) => {}
        other => panic!("expected DupPumpName, got: {other:?}"),
    }

    // With tweak: appends _1, _2
    let p2 = pumps.make("SharedName", true, "LLEventStream").unwrap();
    assert_eq!(p2.name(), "SharedName_1");

    let p3 = pumps.make("SharedName", true, "LLEventStream").unwrap();
    assert_eq!(p3.name(), "SharedName_2");
}

// ============================================================================
// 3. Command Unit Tests
// ============================================================================

#[test]
fn test_command_handshake_intro() {
    let session = LeapSession::with_uuids(
        "54481a53-c41f-4fc2-606e-516daed03636",
        "18ce5015-b651-1d2e-2470-0de841fd3635",
    );

    let intro = session.build_intro_packet();
    assert_eq!(intro.get("pump").as_str(), session.reply_pump);

    let data = intro.get("data");
    assert_eq!(data.get("command").as_str(), session.command_pump);
    assert!(data.get("features").is_map());

    let frame = session.build_intro_frame();
    let mut decoder = LeapDecoder::default();
    decoder.push_bytes(&frame);
    let decoded = decoder.decode_next().unwrap().unwrap();

    assert_eq!(decoded.pump, session.reply_pump);
    assert_eq!(decoded.data.get("command").as_str(), session.command_pump);
}

#[test]
fn test_command_ping_pong_reqid() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    // Ping with integer reqid
    let req_int = llsd_map! {
        "op" => "ping",
        "reply" => session.reply_pump.clone(),
        "reqid" => 777
    };
    let (reply_pump, reply_data) = handler.dispatch(&req_int, &pumps).unwrap();
    assert_eq!(reply_pump, session.reply_pump);
    assert_eq!(reply_data.get("status").as_bool(), true);
    assert_eq!(reply_data.get("reqid").as_i32(), 777);

    // Ping with string reqid
    let req_str = llsd_map! {
        "op" => "ping",
        "reply" => session.reply_pump.clone(),
        "reqid" => "custom_str_id"
    };
    let (_, reply_data_str) = handler.dispatch(&req_str, &pumps).unwrap();
    assert_eq!(reply_data_str.get("status").as_bool(), true);
    assert_eq!(reply_data_str.get("reqid").as_str(), "custom_str_id");

    // Ping with UUID reqid
    let uuid = Uuid::new_v4();
    let req_uuid = llsd_map! {
        "op" => "ping",
        "reply" => session.reply_pump.clone(),
        "reqid" => uuid
    };
    let (_, reply_data_uuid) = handler.dispatch(&req_uuid, &pumps).unwrap();
    assert_eq!(reply_data_uuid.get("status").as_bool(), true);
    assert_eq!(reply_data_uuid.get("reqid").as_uuid(), uuid);
}

#[test]
fn test_command_listen_and_forwarding() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    let forwarded_events = Arc::new(Mutex::new(Vec::new()));
    let fe = forwarded_events.clone();
    handler.set_outbound_sender(move |source, data| {
        fe.lock().push((source.to_string(), data.clone()));
    });

    // Send listen command for 'NotificationStream'
    let listen_cmd = llsd_map! {
        "op" => "listen",
        "source" => "NotificationStream",
        "listener" => "client_listener",
        "reply" => session.reply_pump.clone(),
        "reqid" => 12
    };

    let (_, reply) = handler.dispatch(&listen_cmd, &pumps).unwrap();
    assert_eq!(reply.get("status").as_bool(), true);
    assert_eq!(reply.get("reqid").as_i32(), 12);

    // Duplicate listen should return status: false
    let (_, dup_reply) = handler.dispatch(&listen_cmd, &pumps).unwrap();
    assert_eq!(dup_reply.get("status").as_bool(), false);

    // Post to NotificationStream pump
    pumps
        .obtain("NotificationStream")
        .post(&llsd_map! { "alert" => "Fire!" });

    // Verify outbound sender fired
    assert_eq!(forwarded_events.lock().len(), 1);
    let (src, payload) = &forwarded_events.lock()[0];
    assert_eq!(src, "NotificationStream");
    assert_eq!(payload.get("alert").as_str(), "Fire!");

    // Stop listening
    let stop_cmd = llsd_map! {
        "op" => "stoplistening",
        "source" => "NotificationStream",
        "listener" => "client_listener",
        "reply" => session.reply_pump.clone(),
        "reqid" => 13
    };
    let (_, stop_reply) = handler.dispatch(&stop_cmd, &pumps).unwrap();
    assert_eq!(stop_reply.get("status").as_bool(), true);
    assert_eq!(stop_reply.get("reqid").as_i32(), 13);

    // Stop listening again should return status: false
    let (_, stop_again_reply) = handler.dispatch(&stop_cmd, &pumps).unwrap();
    assert_eq!(stop_again_reply.get("status").as_bool(), false);
}

#[test]
fn test_command_newpump() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    let newpump_cmd = llsd_map! {
        "op" => "newpump",
        "name" => "PuppetryIn",
        "type" => "LLEventStream",
        "reply" => session.reply_pump.clone(),
        "reqid" => 55
    };

    let (_, reply) = handler.dispatch(&newpump_cmd, &pumps).unwrap();
    assert_eq!(reply.get("name").as_str(), "PuppetryIn");
    assert_eq!(reply.get("reqid").as_i32(), 55);

    assert!(pumps.find("PuppetryIn").is_some());
}

#[test]
fn test_command_get_apis_and_get_api() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    // Register a test viewer subsystem API
    let mut viewer_api = ApiMetadata::new("LLTestViewer", "Test viewer API");
    viewer_api.add_operation(
        OperationMetadata::new("getVersion", "Get viewer version")
            .with_optional(&["reply", "reqid"]),
    );
    handler.register_api(viewer_api);

    // getAPIs
    let get_apis_cmd = llsd_map! {
        "op" => "getAPIs",
        "reply" => session.reply_pump.clone(),
        "reqid" => 80
    };
    let (_, apis_reply) = handler.dispatch(&get_apis_cmd, &pumps).unwrap();
    assert_eq!(apis_reply.get("reqid").as_i32(), 80);
    assert!(apis_reply.get("apis").is_map());
    assert!(apis_reply.get("apis").has("LLTestViewer"));
    assert!(apis_reply.has("LLTestViewer")); // Dual compatibility

    // getAPI for LLTestViewer
    let get_api_cmd = llsd_map! {
        "op" => "getAPI",
        "api" => "LLTestViewer",
        "reply" => session.reply_pump.clone(),
        "reqid" => 81
    };
    let (_, api_reply) = handler.dispatch(&get_api_cmd, &pumps).unwrap();
    assert_eq!(api_reply.get("name").as_str(), "LLTestViewer");
    assert_eq!(api_reply.get("reqid").as_i32(), 81);
    assert_eq!(api_reply.get("key").as_str(), "op");
    let ops = api_reply.get("ops").as_array();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].get("name").as_str(), "getVersion");

    // getAPI for unknown API
    let get_unknown_cmd = llsd_map! {
        "op" => "getAPI",
        "api" => "NonExistentAPI",
        "reply" => session.reply_pump.clone(),
        "reqid" => 82
    };
    let (_, unknown_reply) = handler.dispatch(&get_unknown_cmd, &pumps).unwrap();
    assert!(unknown_reply.get("error").as_str().contains("not found"));
    assert_eq!(unknown_reply.get("reqid").as_i32(), 82);
}

#[test]
fn test_command_error_interception_without_panic() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    // Not a map
    let _ = handler.dispatch(&Llsd::Integer(42), &pumps);
    // Note: if request is not a map, dispatch returns Some if reply pump can be extracted or none

    // Missing 'op' key
    let no_op_req = llsd_map! {
        "reply" => session.reply_pump.clone(),
        "reqid" => 90
    };
    let (_, err2) = handler.dispatch(&no_op_req, &pumps).unwrap();
    assert!(err2.get("error").as_str().contains("missing required 'op'"));
    assert_eq!(err2.get("reqid").as_i32(), 90);

    // Unknown operation
    let unknown_op_req = llsd_map! {
        "op" => "flyToMoon",
        "reply" => session.reply_pump.clone(),
        "reqid" => 91
    };
    let (_, err3) = handler.dispatch(&unknown_op_req, &pumps).unwrap();
    assert!(err3.get("error").as_str().contains("unknown operation"));
    assert_eq!(err3.get("reqid").as_i32(), 91);

    // Missing required argument for listen
    let missing_arg_req = llsd_map! {
        "op" => "listen",
        "reply" => session.reply_pump.clone(),
        "reqid" => 92
    };
    let (_, err4) = handler.dispatch(&missing_arg_req, &pumps).unwrap();
    assert!(err4.get("error").as_str().contains("missing required argument"));
    assert_eq!(err4.get("reqid").as_i32(), 92);
}

#[test]
fn test_parse_command_line() {
    let (exe, args) = parse_command_line("python \"test script.py\" --verbose").unwrap();
    assert_eq!(exe, "python");
    assert_eq!(args, vec!["test script.py", "--verbose"]);

    let (exe2, args2) = parse_command_line("  cmd.exe   /C   dir  ").unwrap();
    assert_eq!(exe2, "cmd.exe");
    assert_eq!(args2, vec!["/C", "dir"]);
}

// ============================================================================
// 4. In-Memory MockPipe Duplex Loopback Test
// ============================================================================

struct SharedPipeBuffer {
    data: Mutex<VecDeque<u8>>,
    cvar: Condvar,
    closed: Mutex<bool>,
}

#[derive(Clone)]
struct MockPipeWriter {
    shared: Arc<SharedPipeBuffer>,
}

#[derive(Clone)]
struct MockPipeReader {
    shared: Arc<SharedPipeBuffer>,
}

fn create_mock_pipe() -> (MockPipeReader, MockPipeWriter) {
    let shared = Arc::new(SharedPipeBuffer {
        data: Mutex::new(VecDeque::new()),
        cvar: Condvar::new(),
        closed: Mutex::new(false),
    });

    (
        MockPipeReader {
            shared: shared.clone(),
        },
        MockPipeWriter { shared },
    )
}

impl Write for MockPipeWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let mut data = self.shared.data.lock();
        data.extend(buf);
        self.shared.cvar.notify_all();
        Ok(buf.len())
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl Drop for MockPipeWriter {
    fn drop(&mut self) {
        *self.shared.closed.lock() = true;
        self.shared.cvar.notify_all();
    }
}

impl Read for MockPipeReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let mut data = self.shared.data.lock();
        loop {
            if !data.is_empty() {
                let n = data.len().min(buf.len());
                for b in buf.iter_mut().take(n) {
                    *b = data.pop_front().unwrap();
                }
                return Ok(n);
            }

            if *self.shared.closed.lock() {
                return Ok(0); // EOF
            }

            self.shared.cvar.wait(&mut data);
        }
    }
}

impl MockPipeReader {
    fn read_timeout(&mut self, buf: &mut [u8], timeout: Duration) -> IoResult<usize> {
        let mut data = self.shared.data.lock();
        if !data.is_empty() {
            let n = data.len().min(buf.len());
            for b in buf.iter_mut().take(n) {
                *b = data.pop_front().unwrap();
            }
            return Ok(n);
        }

        if *self.shared.closed.lock() {
            return Ok(0);
        }

        self.shared.cvar.wait_for(&mut data, timeout);
        if !data.is_empty() {
            let n = data.len().min(buf.len());
            for b in buf.iter_mut().take(n) {
                *b = data.pop_front().unwrap();
            }
            Ok(n)
        } else {
            Ok(0)
        }
    }
}

#[test]
fn test_end_to_end_mock_pipe_loopback() {
    // Pipe 1: Viewer stdout (writer) -> Client stdin (reader)
    let (mut client_reader, viewer_writer) = create_mock_pipe();

    // Pipe 2: Client stdout (writer) -> Viewer stdin (reader)
    let (viewer_reader, mut client_writer) = create_mock_pipe();

    let session = LeapSession::with_uuids(
        "reply-uuid-1111-2222",
        "command-uuid-3333-4444",
    );

    // Initialize LeapManager
    let mut manager = LeapManager::from_streams_with_session(
        "mock-test",
        session.clone(),
        viewer_reader,
        viewer_writer,
        None::<std::io::Empty>,
    )
    .expect("failed to init LeapManager");

    // 1. Client reads initial handshake intro packet
    let mut client_decoder = LeapDecoder::default();
    let mut intro_msg = None;

    let mut buf = [0u8; 1024];
    for _ in 0..50 {
        if let Ok(n) = client_reader.read_timeout(&mut buf, Duration::from_millis(20)) {
            if n > 0 {
                client_decoder.push_bytes(&buf[..n]);
                if let Ok(Some(msg)) = client_decoder.decode_next() {
                    intro_msg = Some(msg);
                    break;
                }
            }
        }
    }

    let intro = intro_msg.expect("client did not receive intro handshake packet");
    assert_eq!(intro.pump, session.reply_pump);
    assert_eq!(intro.data.get("command").as_str(), session.command_pump);

    // 2. Client sends ping command to command_pump
    let ping_req = LeapMessage::new(
        &session.command_pump,
        llsd_map! {
            "op" => "ping",
            "reply" => session.reply_pump.clone(),
            "reqid" => 54321
        },
    );
    client_writer
        .write_all(&LeapEncoder::encode(&ping_req))
        .unwrap();
    client_writer.flush().unwrap();

    // Viewer polls try_recv() to process command
    let mut handled = false;
    for _ in 0..50 {
        manager.try_recv();
        if let Ok(n) = client_reader.read_timeout(&mut buf, Duration::from_millis(20)) {
            if n > 0 {
                client_decoder.push_bytes(&buf[..n]);
                if let Ok(Some(msg)) = client_decoder.decode_next() {
                    assert_eq!(msg.pump, session.reply_pump);
                    assert_eq!(msg.data.get("status").as_bool(), true);
                    assert_eq!(msg.data.get("reqid").as_i32(), 54321);
                    handled = true;
                    break;
                }
            }
        }
    }
    assert!(handled, "client did not receive ping response");

    // 3. Client subscribes to viewer event pump "Chat" via listen command
    let listen_req = LeapMessage::new(
        &session.command_pump,
        llsd_map! {
            "op" => "listen",
            "source" => "Chat",
            "listener" => "client_chat_listener",
            "reply" => session.reply_pump.clone(),
            "reqid" => 600
        },
    );
    client_writer
        .write_all(&LeapEncoder::encode(&listen_req))
        .unwrap();
    client_writer.flush().unwrap();

    let mut listen_confirmed = false;
    for _ in 0..50 {
        manager.try_recv();
        if let Ok(n) = client_reader.read_timeout(&mut buf, Duration::from_millis(20)) {
            if n > 0 {
                client_decoder.push_bytes(&buf[..n]);
                if let Ok(Some(msg)) = client_decoder.decode_next() {
                    if msg.pump == session.reply_pump && msg.data.get("reqid").as_i32() == 600 {
                        assert_eq!(msg.data.get("status").as_bool(), true);
                        listen_confirmed = true;
                        break;
                    }
                }
            }
        }
    }
    assert!(listen_confirmed, "client did not receive listen confirmation");

    // 4. Viewer posts event to "Chat" pump; client should receive it decorated as {"pump": "Chat", "data": ...}
    manager.post(
        "Chat",
        llsd_map! {
            "from" => "Tester Resident",
            "message" => "Hello over LEAP!"
        },
    );

    let mut chat_received = false;
    for _ in 0..50 {
        manager.try_recv();
        if let Ok(n) = client_reader.read_timeout(&mut buf, Duration::from_millis(20)) {
            if n > 0 {
                client_decoder.push_bytes(&buf[..n]);
                if let Ok(Some(msg)) = client_decoder.decode_next() {
                    if msg.pump == "Chat" {
                        assert_eq!(msg.data.get("from").as_str(), "Tester Resident");
                        assert_eq!(msg.data.get("message").as_str(), "Hello over LEAP!");
                        chat_received = true;
                        break;
                    }
                }
            }
        }
    }
    assert!(chat_received, "client did not receive forwarded Chat event");

    // 5. Client sends message to a subsystem pump "LLViewerControl"
    let subsystem_msg = LeapMessage::new(
        "LLViewerControl",
        llsd_map! {
            "op" => "get",
            "key" => "AutoConnect"
        },
    );
    client_writer
        .write_all(&LeapEncoder::encode(&subsystem_msg))
        .unwrap();
    client_writer.flush().unwrap();

    // Viewer try_recv() should return this subsystem message
    let mut subsystem_received = false;
    for _ in 0..50 {
        if let Some(msg) = manager.try_recv() {
            if msg.pump == "LLViewerControl" {
                assert_eq!(msg.data.get("op").as_str(), "get");
                assert_eq!(msg.data.get("key").as_str(), "AutoConnect");
                subsystem_received = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        subsystem_received,
        "viewer did not receive subsystem message from client"
    );

    // Clean shutdown
    manager.shutdown();
}
