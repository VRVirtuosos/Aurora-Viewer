//! Adversarial Concurrency, Protocol, and Stress Verification for `aurora-leap`.
//!
//! Authored by Challenger 2 for Milestone 2.
//!
//! Empirically tests:
//! 1. Pub/sub distribution under multi-threaded concurrency (heavy simultaneous posting & listening).
//! 2. MailDrop queueing under race conditions (events posted before listeners attach, concurrent posting/attaching).
//! 3. Duplicate listener rejection (`status: false`) under concurrent registration races.
//! 4. `reqid` preservation under diverse data types (integer, string, UUID, float, bool, binary, array, map, date, undef) with wire framing round-trip.
//! 5. Robust error handling of unknown pumps, unknown commands, missing keys, and exception recovery without viewer crashes.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use uuid::Uuid;

use aurora_leap::codec::{LeapDecoder, LeapEncoder, LeapMessage};
use aurora_leap::command::{stamp_reqid, ApiMetadata, LeapCommandHandler, LeapSession, OperationMetadata};
use aurora_leap::pump::EventPumps;
use aurora_llsd::{llsd_map, Llsd};

// ============================================================================
// 1. Pub/Sub Multi-Threaded Concurrency Tests
// ============================================================================

#[test]
fn test_concurrency_multithreaded_pubsub_heavy_load() {
    let pumps = EventPumps::new();
    let pump = pumps.obtain("ConcurrentChat");

    const NUM_PRODUCERS: usize = 8;
    const MSGS_PER_PRODUCER: usize = 500;
    const NUM_CONSUMERS: usize = 4;
    const EXPECTED_TOTAL: usize = NUM_PRODUCERS * MSGS_PER_PRODUCER;

    let received_counts: Vec<Arc<AtomicUsize>> = (0..NUM_CONSUMERS)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();

    // Attach consumers
    for (idx, count) in received_counts.iter().enumerate() {
        let cnt = count.clone();
        let listener_name = format!("listener_{idx}");
        assert!(pump.listen(&listener_name, move |_event| {
            cnt.fetch_add(1, Ordering::Relaxed);
            false // Do not consume, broadcast to all
        }));
    }

    let start_barrier = Arc::new(Barrier::new(NUM_PRODUCERS));
    let mut handles = Vec::new();

    // Spawn producer threads posting simultaneously
    for prod_id in 0..NUM_PRODUCERS {
        let p = pump.clone();
        let barrier = start_barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for seq in 0..MSGS_PER_PRODUCER {
                let msg = llsd_map! {
                    "producer" => prod_id as i32,
                    "seq" => seq as i32,
                    "data" => "concurrency payload"
                };
                p.post(&msg);
            }
        }));
    }

    for h in handles {
        h.join().expect("producer thread panicked");
    }

    // Every persistent consumer must receive 100% of the broadcast messages
    for (idx, count) in received_counts.iter().enumerate() {
        let val = count.load(Ordering::SeqCst);
        assert_eq!(
            val, EXPECTED_TOTAL,
            "consumer {idx} received {val} events, expected {EXPECTED_TOTAL}"
        );
    }
}

#[test]
fn test_concurrency_simultaneous_listen_stop_listening_post() {
    let pumps = EventPumps::new();
    let pump = pumps.obtain("ChurnPump");
    let stop_flag = Arc::new(AtomicBool::new(false));

    // 4 poster threads
    let mut poster_handles = Vec::new();
    for prod_id in 0..4 {
        let p = pump.clone();
        let stop = stop_flag.clone();
        poster_handles.push(thread::spawn(move || {
            let mut seq = 0;
            while !stop.load(Ordering::Relaxed) {
                p.post(&llsd_map! { "p" => prod_id, "s" => seq });
                seq += 1;
            }
        }));
    }

    // 4 churn threads attaching and detaching listeners
    let mut churn_handles = Vec::new();
    for churn_id in 0..4 {
        let p = pump.clone();
        let stop = stop_flag.clone();
        churn_handles.push(thread::spawn(move || {
            let name = format!("churn_listener_{churn_id}");
            while !stop.load(Ordering::Relaxed) {
                let connected = p.listen(&name, |_| false);
                if connected {
                    // Let it observe a few events
                    thread::yield_now();
                    p.stop_listening(&name);
                }
            }
        }));
    }

    // Let the churn run for 200 ms under high contention
    thread::sleep(Duration::from_millis(200));
    stop_flag.store(true, Ordering::SeqCst);

    for h in poster_handles {
        h.join().expect("poster thread panicked under churn");
    }
    for h in churn_handles {
        h.join().expect("churn thread panicked");
    }

    // Must be completely free of deadlocks and valid state
    assert!(pump.enabled());
}

#[test]
fn test_concurrency_cross_pump_nested_deadlock_stress() {
    let pumps = EventPumps::new();
    let pump_a = pumps.obtain("ChainA");
    let pump_b = pumps.obtain("ChainB");
    let pump_c = pumps.obtain("ChainC");

    let p_b = pump_b.clone();
    pump_a.listen("a_to_b", move |ev| {
        p_b.post(ev);
        false
    });

    let p_c = pump_c.clone();
    pump_b.listen("b_to_c", move |ev| {
        p_c.post(ev);
        false
    });

    let c_delivered = Arc::new(AtomicUsize::new(0));
    let c_cnt = c_delivered.clone();
    pump_c.listen("c_sink", move |_| {
        c_cnt.fetch_add(1, Ordering::Relaxed);
        true
    });

    // 8 threads posting to ChainA simultaneously
    let mut handles = Vec::new();
    for i in 0..8 {
        let p_a = pump_a.clone();
        handles.push(thread::spawn(move || {
            for j in 0..200 {
                p_a.post(&llsd_map! { "thread" => i, "seq" => j });
            }
        }));
    }

    for h in handles {
        h.join().expect("nested chain thread panicked");
    }

    assert_eq!(c_delivered.load(Ordering::SeqCst), 8 * 200);
}

// ============================================================================
// 2. MailDrop Queueing & Race Condition Tests
// ============================================================================

#[test]
fn test_maildrop_queueing_offline_delivery_fifo() {
    let pumps = EventPumps::new();
    let maildrop = pumps
        .make("OfflineMail", false, "LLEventMailDrop")
        .expect("failed to make maildrop");

    // Post 10 events before any listener attaches
    for i in 0..10 {
        maildrop.post(&llsd_map! { "order" => i });
    }
    assert_eq!(maildrop.history_len(), 10);

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let del = delivered.clone();

    // Attaching listener flushes all 10 in strict FIFO order
    maildrop.listen("drainer", move |ev| {
        del.lock().push(ev.get("order").as_i32());
        true // consume
    });

    assert_eq!(maildrop.history_len(), 0);
    let history = delivered.lock().clone();
    assert_eq!(history, (0..10).collect::<Vec<i32>>());
}

#[test]
fn test_maildrop_partial_consumption_and_second_listener() {
    let pumps = EventPumps::new();
    let maildrop = pumps.make("PartialDrop", false, "LLEventMailDrop").unwrap();

    maildrop.post(&llsd_map! { "val" => 1 });
    maildrop.post(&llsd_map! { "val" => 2 });
    maildrop.post(&llsd_map! { "val" => 3 });
    maildrop.post(&llsd_map! { "val" => 4 });
    assert_eq!(maildrop.history_len(), 4);

    // Listener 1 consumes only even numbers
    maildrop.listen("even_consumer", |ev| {
        let v = ev.get("val").as_i32();
        v % 2 == 0
    });

    // 1 and 3 should remain in history
    assert_eq!(maildrop.history_len(), 2);

    let remaining = Arc::new(Mutex::new(Vec::new()));
    let rem = remaining.clone();

    // Listener 2 attaches and consumes everything
    maildrop.listen("cleanup_consumer", move |ev| {
        rem.lock().push(ev.get("val").as_i32());
        true
    });

    assert_eq!(maildrop.history_len(), 0);
    assert_eq!(*remaining.lock(), vec![1, 3]);
}

#[test]
fn test_maildrop_concurrent_posting_and_listener_attachment() {
    // Stress test: concurrent posting vs listener attachment
    let pumps = EventPumps::new();
    let maildrop = pumps.make("RaceDrop", false, "LLEventMailDrop").unwrap();

    const COUNT: usize = 500;
    let md_post = maildrop.clone();

    let post_handle = thread::spawn(move || {
        for i in 0..COUNT {
            md_post.post(&llsd_map! { "id" => i as i32 });
        }
    });

    let consumed = Arc::new(AtomicUsize::new(0));
    let cons = consumed.clone();

    // Small delay so some events go to history, then attach listener
    thread::sleep(Duration::from_millis(1));
    maildrop.listen("race_listener", move |_| {
        cons.fetch_add(1, Ordering::Relaxed);
        true
    });

    post_handle.join().unwrap();

    // Total events either consumed or remaining in history must equal COUNT
    let total = consumed.load(Ordering::SeqCst) + maildrop.history_len();
    assert_eq!(
        total, COUNT,
        "event loss detected in maildrop: consumed={}, in_history={}, expected={COUNT}",
        consumed.load(Ordering::SeqCst),
        maildrop.history_len()
    );
}

// ============================================================================
// 3. Duplicate Listener Rejection & Registration Races
// ============================================================================

#[test]
fn test_duplicate_listener_rejection_direct_and_command() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    let pump = pumps.obtain("DupTestPump");

    // Direct registration
    assert!(pump.listen("sub_one", |_| false));
    // Duplicate rejection
    assert_eq!(pump.listen("sub_one", |_| false), false);

    // Protocol command listen
    let listen_cmd = llsd_map! {
        "op" => "listen",
        "source" => "DupTestPump",
        "listener" => "proto_sub",
        "reply" => session.reply_pump.clone(),
        "reqid" => 100
    };

    let (_, rep1) = handler.dispatch(&listen_cmd, &pumps).unwrap();
    assert_eq!(rep1.get("status").as_bool(), true);
    assert_eq!(rep1.get("reqid").as_i32(), 100);

    // Duplicate listen command -> status: false
    let (_, rep2) = handler.dispatch(&listen_cmd, &pumps).unwrap();
    assert_eq!(rep2.get("status").as_bool(), false);
    assert_eq!(rep2.get("reqid").as_i32(), 100);

    // Stop listening
    let stop_cmd = llsd_map! {
        "op" => "stoplistening",
        "source" => "DupTestPump",
        "listener" => "proto_sub",
        "reply" => session.reply_pump.clone(),
        "reqid" => 101
    };
    let (_, stop_rep1) = handler.dispatch(&stop_cmd, &pumps).unwrap();
    assert_eq!(stop_rep1.get("status").as_bool(), true);

    // Stop listening non-existent / already stopped -> status: false
    let (_, stop_rep2) = handler.dispatch(&stop_cmd, &pumps).unwrap();
    assert_eq!(stop_rep2.get("status").as_bool(), false);

    // Re-registration now succeeds
    let (_, rep3) = handler.dispatch(&listen_cmd, &pumps).unwrap();
    assert_eq!(rep3.get("status").as_bool(), true);
}

#[test]
fn test_concurrency_race_to_register_same_listener_name() {
    let pumps = EventPumps::new();
    let pump = pumps.obtain("ContestedPump");

    const NUM_THREADS: usize = 20;
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let successes = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..NUM_THREADS {
        let p = pump.clone();
        let b = barrier.clone();
        let s = successes.clone();
        handles.push(thread::spawn(move || {
            b.wait();
            if p.listen("exclusive_listener", |_| false) {
                s.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // EXACTLY one thread must have succeeded
    assert_eq!(
        successes.load(Ordering::SeqCst),
        1,
        "exactly one thread should win the registration race"
    );
    assert_eq!(pump.listener_names().len(), 1);
}

// ============================================================================
// 4. `reqid` Preservation Under Diverse Data Types & Full Round-Trip
// ============================================================================

#[test]
fn test_reqid_preservation_diverse_types_direct_and_wire() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    let test_uuids = [Uuid::nil(), Uuid::new_v4()];

    let test_cases: Vec<(&str, Llsd)> = vec![
        ("int_zero", Llsd::Integer(0)),
        ("int_pos", Llsd::Integer(123456789)),
        ("int_neg", Llsd::Integer(-987654321)),
        ("str_empty", Llsd::String("".into())),
        ("str_ascii", Llsd::String("request-token-1234".into())),
        ("str_unicode", Llsd::String("req-🔥-utf8".into())),
        ("uuid_nil", Llsd::Uuid(test_uuids[0])),
        ("uuid_v4", Llsd::Uuid(test_uuids[1])),
        ("real_zero", Llsd::Real(0.0)),
        ("real_pi", Llsd::Real(3.141592653589793)),
        ("real_neg", Llsd::Real(-273.15)),
        ("bool_true", Llsd::Boolean(true)),
        ("bool_false", Llsd::Boolean(false)),
        ("binary", Llsd::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x42])),
        (
            "array",
            Llsd::Array(vec![
                Llsd::Integer(1),
                Llsd::String("sub_id".into()),
                Llsd::Real(2.5),
            ]),
        ),
        (
            "map",
            llsd_map! {
                "sub_key" => "sub_val",
                "nested_num" => 42
            },
        ),
        ("date", Llsd::Date(1600000000.0)),
    ];

    for (name, reqid_val) in test_cases {
        // 1. In-memory stamp_reqid test
        let mut reply = llsd_map! { "status" => true };
        let request = llsd_map! { "reqid" => reqid_val.clone() };
        stamp_reqid(&mut reply, &request);
        assert_eq!(
            reply.get("reqid"),
            &reqid_val,
            "stamp_reqid failed for {name}"
        );

        // 2. Full wire-framed dispatch test
        let wire_msg = LeapMessage::new(
            &session.command_pump,
            llsd_map! {
                "op" => "ping",
                "reply" => session.reply_pump.clone(),
                "reqid" => reqid_val.clone()
            },
        );

        let encoded_req = LeapEncoder::encode(&wire_msg);
        let mut decoder = LeapDecoder::default();
        decoder.push_bytes(&encoded_req);
        let decoded_req = decoder.decode_next().unwrap().unwrap();

        // Dispatch
        let (rep_pump, rep_data) = handler.dispatch(&decoded_req.data, &pumps).unwrap();
        assert_eq!(rep_pump, session.reply_pump);

        // Wire-encode reply
        let reply_wire = LeapMessage::new(rep_pump, rep_data);
        let encoded_rep = LeapEncoder::encode(&reply_wire);

        let mut rep_decoder = LeapDecoder::default();
        rep_decoder.push_bytes(&encoded_rep);
        let decoded_rep = rep_decoder.decode_next().unwrap().unwrap();

        assert_eq!(
            decoded_rep.data.get("reqid"),
            &reqid_val,
            "wire roundtrip reqid mismatch for {name}"
        );
    }
}

#[test]
fn test_reqid_absent_or_undef_not_stamped() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    // No reqid in request
    let req_no_reqid = llsd_map! {
        "op" => "ping",
        "reply" => session.reply_pump.clone()
    };
    let (_, rep1) = handler.dispatch(&req_no_reqid, &pumps).unwrap();
    assert!(rep1.get("reqid").is_undef(), "reqid should not be present");

    // Undef reqid in request
    let req_undef = llsd_map! {
        "op" => "ping",
        "reply" => session.reply_pump.clone(),
        "reqid" => Llsd::Undef
    };
    let (_, rep2) = handler.dispatch(&req_undef, &pumps).unwrap();
    assert!(
        rep2.get("reqid").is_undef(),
        "undef reqid should not be stamped"
    );
}

// ============================================================================
// 5. Safe Handling of Unknown Pumps, Commands, and Error Recovery
// ============================================================================

#[test]
fn test_error_handling_unknown_commands_without_crash() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    let strange_ops = vec![
        "",
        "unknown_op_foo",
        "12345",
        "__system_hack__",
        "../../../escaped",
    ];

    for op in strange_ops {
        let req = llsd_map! {
            "op" => op,
            "reply" => session.reply_pump.clone(),
            "reqid" => 404
        };
        let (rep_pump, rep_data) = handler.dispatch(&req, &pumps).unwrap();
        assert_eq!(rep_pump, session.reply_pump);
        assert_eq!(rep_data.get("reqid").as_i32(), 404);
        assert!(
            rep_data.has("error"),
            "operation '{op}' should have returned structured error"
        );
    }
}

#[test]
fn test_error_handling_missing_arguments_without_crash() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    // Missing 'op'
    let no_op = llsd_map! { "reply" => session.reply_pump.clone(), "reqid" => 1 };
    let (_, rep_no_op) = handler.dispatch(&no_op, &pumps).unwrap();
    assert!(rep_no_op.get("error").as_str().contains("missing required 'op'"));

    // 'listen' missing 'source'
    let listen_no_src = llsd_map! {
        "op" => "listen",
        "listener" => "my_sub",
        "reply" => session.reply_pump.clone(),
        "reqid" => 2
    };
    let (_, rep2) = handler.dispatch(&listen_no_src, &pumps).unwrap();
    assert!(rep2.get("error").as_str().contains("source"));

    // 'listen' missing 'listener'
    let listen_no_sub = llsd_map! {
        "op" => "listen",
        "source" => "my_pump",
        "reply" => session.reply_pump.clone(),
        "reqid" => 3
    };
    let (_, rep3) = handler.dispatch(&listen_no_sub, &pumps).unwrap();
    assert!(rep3.get("error").as_str().contains("listener"));

    // 'stoplistening' missing 'source'
    let stop_no_src = llsd_map! {
        "op" => "stoplistening",
        "listener" => "my_sub",
        "reply" => session.reply_pump.clone(),
        "reqid" => 4
    };
    let (_, rep4) = handler.dispatch(&stop_no_src, &pumps).unwrap();
    assert!(rep4.get("error").as_str().contains("source"));

    // 'newpump' missing 'name'
    let newpump_no_name = llsd_map! {
        "op" => "newpump",
        "reply" => session.reply_pump.clone(),
        "reqid" => 5
    };
    let (_, rep5) = handler.dispatch(&newpump_no_name, &pumps).unwrap();
    assert!(rep5.get("error").as_str().contains("name"));

    // 'newpump' with invalid pump type
    let newpump_bad_type = llsd_map! {
        "op" => "newpump",
        "name" => "BadTypePump",
        "type" => "LLInvalidPumpType",
        "reply" => session.reply_pump.clone(),
        "reqid" => 6
    };
    let (_, rep6) = handler.dispatch(&newpump_bad_type, &pumps).unwrap();
    assert!(rep6.get("error").as_str().contains("unsupported pump type"));

    // 'getAPI' missing 'api'
    let get_api_no_arg = llsd_map! {
        "op" => "getAPI",
        "reply" => session.reply_pump.clone(),
        "reqid" => 7
    };
    let (_, rep7) = handler.dispatch(&get_api_no_arg, &pumps).unwrap();
    assert!(rep7.get("error").as_str().contains("api"));

    // 'getAPI' non-existent API
    let get_api_unknown = llsd_map! {
        "op" => "getAPI",
        "api" => "NonExistentAPI_XYZ",
        "reply" => session.reply_pump.clone(),
        "reqid" => 8
    };
    let (_, rep8) = handler.dispatch(&get_api_unknown, &pumps).unwrap();
    assert!(rep8.get("error").as_str().contains("not found"));

    // 'getFeature' missing 'feature'
    let get_feat_no_arg = llsd_map! {
        "op" => "getFeature",
        "reply" => session.reply_pump.clone(),
        "reqid" => 9
    };
    let (_, rep9) = handler.dispatch(&get_feat_no_arg, &pumps).unwrap();
    assert!(rep9.get("error").as_str().contains("feature"));

    // 'getFeature' unknown feature -> returns empty map without error
    let get_feat_unknown = llsd_map! {
        "op" => "getFeature",
        "feature" => "unknown_feature",
        "reply" => session.reply_pump.clone(),
        "reqid" => 10
    };
    let (_, rep10) = handler.dispatch(&get_feat_unknown, &pumps).unwrap();
    assert!(!rep10.has("error"));
    assert_eq!(rep10.get("reqid").as_i32(), 10);
}

#[test]
fn test_error_handling_non_map_request_payloads() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    // Passing non-map payloads (integer, string, array, boolean, undef)
    assert!(handler.dispatch(&Llsd::Integer(42), &pumps).is_none());
    assert!(handler
        .dispatch(&Llsd::String("plain string".into()), &pumps)
        .is_none());
    assert!(handler
        .dispatch(&Llsd::Array(vec![Llsd::Integer(1)]), &pumps)
        .is_none());
    assert!(handler.dispatch(&Llsd::Boolean(false), &pumps).is_none());
    assert!(handler.dispatch(&Llsd::Undef, &pumps).is_none());
}

#[test]
fn test_unknown_pump_posting_safety() {
    let pumps = EventPumps::new();

    // Posting to a pump that was never created
    let res = pumps.post("CompletelyUnknownPump", &llsd_map! { "x" => 1 });
    assert_eq!(res, false, "posting to unknown pump should return false");

    // Stopping listener on an unknown pump
    let stop_res = pumps.stop_listening("CompletelyUnknownPump", "some_listener");
    assert_eq!(
        stop_res, false,
        "stop_listening on unknown pump should return false"
    );

    // Finding unknown pump
    assert!(pumps.find("CompletelyUnknownPump").is_none());
}

// ============================================================================
// 6. API Introspection Completeness Tests
// ============================================================================

#[test]
fn test_api_introspection_builtin_and_subsystem() {
    let session = LeapSession::new();
    let mut handler = LeapCommandHandler::new(session.clone());
    let pumps = EventPumps::new();

    // 1. Introspect built-in command pump API
    let get_builtin = llsd_map! {
        "op" => "getAPI",
        "api" => session.command_pump.clone(),
        "reply" => session.reply_pump.clone(),
        "reqid" => 200
    };
    let (_, builtin_api) = handler.dispatch(&get_builtin, &pumps).unwrap();
    assert_eq!(builtin_api.get("name").as_str(), session.command_pump);
    assert_eq!(builtin_api.get("key").as_str(), "op");
    assert_eq!(builtin_api.get("reqid").as_i32(), 200);

    let ops = builtin_api.get("ops").as_array();
    let op_names: HashSet<String> = ops
        .iter()
        .map(|op| op.get("name").as_str().to_string())
        .collect();

    let required_ops = [
        "ping",
        "listen",
        "stoplistening",
        "newpump",
        "getAPIs",
        "getAPI",
        "getFeatures",
        "getFeature",
    ];
    for op in required_ops {
        assert!(
            op_names.contains(op),
            "built-in API missing documented operation '{op}'"
        );
    }

    // 2. Register custom subsystem API and verify getAPIs
    let mut custom_api = ApiMetadata::new("LLCustomSubsystem", "Test subsystem description");
    custom_api.add_operation(
        OperationMetadata::new("executeCustom", "Executes custom command")
            .with_required(&["param1", "param2"])
            .with_optional(&["opt_flag"]),
    );
    handler.register_api(custom_api);

    let get_all = llsd_map! {
        "op" => "getAPIs",
        "reply" => session.reply_pump.clone(),
        "reqid" => 201
    };
    let (_, apis_rep) = handler.dispatch(&get_all, &pumps).unwrap();
    assert_eq!(apis_rep.get("reqid").as_i32(), 201);

    // Verify both "apis" submap and root map contain the registered API
    let apis_submap = apis_rep.get("apis");
    assert!(apis_submap.is_map());
    assert!(apis_submap.has("LLCustomSubsystem"));
    assert_eq!(
        apis_submap.get("LLCustomSubsystem").get("desc").as_str(),
        "Test subsystem description"
    );
    assert!(apis_rep.has("LLCustomSubsystem")); // Dual compatibility

    // Verify operation details for LLCustomSubsystem
    let get_custom = llsd_map! {
        "op" => "getAPI",
        "api" => "LLCustomSubsystem",
        "reply" => session.reply_pump.clone(),
        "reqid" => 202
    };
    let (_, custom_rep) = handler.dispatch(&get_custom, &pumps).unwrap();
    assert_eq!(custom_rep.get("name").as_str(), "LLCustomSubsystem");
    let c_ops = custom_rep.get("ops").as_array();
    assert_eq!(c_ops.len(), 1);
    assert_eq!(c_ops[0].get("name").as_str(), "executeCustom");

    let req_args = c_ops[0].get("required").as_array();
    assert_eq!(req_args.len(), 2);
    assert_eq!(req_args[0].as_str(), "param1");
    assert_eq!(req_args[1].as_str(), "param2");

    let opt_args = c_ops[0].get("optional").as_array();
    assert_eq!(opt_args.len(), 1);
    assert_eq!(opt_args[0].as_str(), "opt_flag");
}
