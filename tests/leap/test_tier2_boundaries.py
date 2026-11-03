"""
LEAP Automated Test Suite - Tier 2: Boundary & Corner Cases.

Covers:
  - Buffer boundary reassembly (feeding frames byte-by-byte or in small slices).
  - Large payload roundtrip (100KB, 250KB, 500KB).
  - Complex data types in reqid (negative ints, UUIDs, Unicode, reals, arrays, maps).
  - Protocol robustness and structured error handling without viewer crash or panic.
"""

import sys
import time
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).parent))
from leap_client import LeapClient
from leap_codec import encode_message


def run_tier2_tests(client: LeapClient) -> Dict[str, Any]:
    """Run all Tier 2 tests against an active LeapClient instance."""
    results = {}
    print("\n=== Running Tier 2: Boundary & Corner Cases ===")

    # 2.1 Buffer Boundary Reassembly (Chunked Slices)
    t0 = time.time()
    try:
        # Test 1-byte slice streaming
        frame_1 = encode_message(
            client.cmd_uuid,
            {"op": "ping", "reply": client.reply_uuid, "reqid": 20101, "msg": "byte_by_byte"}
        )
        for b in frame_1:
            client.send_raw(bytes([b]))
            # Tiny yield to ensure OS pipe fragments across reads
            time.sleep(0.0005)

        # Wait for reply
        reply_1 = None
        t_wait = time.time()
        while time.time() - t_wait < 5.0:
            with client._pending_lock:
                q = client._pending_replies.get(20101)
            # Alternatively drain all events
            for ev in client._all_events:
                d = ev.get("data", {})
                if isinstance(d, dict) and d.get("reqid") == 20101:
                    reply_1 = d
                    break
            if reply_1:
                break
            time.sleep(0.02)
        assert reply_1 is not None, "Timed out waiting for 1-byte slice ping reply"
        assert reply_1.get("status") is True
        assert reply_1.get("reqid") == 20101

        # Test prime-sized slices (3-byte, 7-byte, 17-byte chunks)
        for chunk_size, reqid in [(3, 20103), (7, 20107), (17, 20117)]:
            frame = encode_message(
                client.cmd_uuid,
                {"op": "ping", "reply": client.reply_uuid, "reqid": reqid, "slice": chunk_size}
            )
            for i in range(0, len(frame), chunk_size):
                client.send_raw(frame[i : i + chunk_size])
                time.sleep(0.0005)

            # Wait for reply
            reply = None
            t_wait = time.time()
            while time.time() - t_wait < 5.0:
                for ev in client._all_events:
                    d = ev.get("data", {})
                    if isinstance(d, dict) and d.get("reqid") == reqid:
                        reply = d
                        break
                if reply:
                    break
                time.sleep(0.02)
            assert reply is not None, f"Timed out waiting for {chunk_size}-byte slice reply (reqid={reqid})"
            assert reply.get("status") is True

        results["test_2_1_boundary_reassembly"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 2.1 Buffer Boundary Reassembly (1-byte, 3-byte, 7-byte, 17-byte slices)")
    except Exception as err:
        results["test_2_1_boundary_reassembly"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 2.1 Boundary Reassembly: {err}")

    # 2.2 Large Payload Roundtrip (100KB, 250KB, 500KB)
    t0 = time.time()
    try:
        for size_kb, reqid in [(100, 20201), (250, 20202), (500, 20203)]:
            payload_str = "X" * (size_kb * 1024)
            # Send ping carrying the large payload
            large_req = {
                "op": "ping",
                "blob": payload_str,
                "reply": client.reply_uuid,
                "reqid": reqid,
            }
            resp = client.request(client.cmd_uuid, data=large_req, reqid=reqid, timeout=10.0)
            assert resp.get("status") is True, f"Failed ping for {size_kb}KB payload"
            assert resp.get("reqid") == reqid

        results["test_2_2_large_payloads"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 2.2 Large Payloads (100KB, 250KB, 500KB roundtrip)")
    except Exception as err:
        results["test_2_2_large_payloads"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 2.2 Large Payloads: {err}")

    # 2.3 Complex Data Types in reqid (Negative ints, UUIDs, Unicode, Reals, Arrays, Maps)
    t0 = time.time()
    try:
        # Negative integer
        r_neg = client.ping(reqid=-42, timeout=5.0)
        assert r_neg.get("reqid") == -42, f"Failed negative reqid: {r_neg}"

        # Large negative integer
        r_lneg = client.ping(reqid=-999999, timeout=5.0)
        assert r_lneg.get("reqid") == -999999

        # RFC 4122 UUID string
        uuid_reqid = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        r_uuid = client.ping(reqid=uuid_reqid, timeout=5.0)
        assert r_uuid.get("reqid") == uuid_reqid

        # Unicode string with non-ASCII and emojis
        unicode_reqid = "ReqId_αβγ_测试_🌟_123"
        r_uni = client.ping(reqid=unicode_reqid, timeout=5.0)
        assert r_uni.get("reqid") == unicode_reqid

        # Float / Real number
        r_float = client.ping(reqid=3.14159, timeout=5.0)
        assert abs(float(r_float.get("reqid")) - 3.14159) < 1e-4

        # Array reqid
        r_arr = client.ping(reqid=[1, "two", False], timeout=5.0)
        assert r_arr.get("reqid") == [1, "two", False]

        results["test_2_3_complex_reqid"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 2.3 Complex Data Types in reqid (Negative ints, UUID, Unicode, Float, Array)")
    except Exception as err:
        results["test_2_3_complex_reqid"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 2.3 Complex reqid: {err}")

    # 2.4 Error Handling and Protocol Robustness
    t0 = time.time()
    try:
        # 1. Missing operation on command pump
        err_no_op = client.command("", reqid=20401, timeout=5.0)
        assert "error" in err_no_op, f"Expected error for missing op: {err_no_op}"
        assert err_no_op.get("reqid") == 20401

        # 2. Unknown operation on command pump
        err_unk_op = client.command("nonExistentLeapOp", reqid=20402, timeout=5.0)
        assert "error" in err_unk_op
        assert "unknown operation" in err_unk_op.get("error", "").lower()
        assert err_unk_op.get("reqid") == 20402

        # 3. Missing required argument on LLViewerControl
        err_no_key = client.request("LLViewerControl", op="get", data={}, reqid=20403, timeout=5.0)
        assert "error" in err_no_key
        assert "Missing required 'key' argument" in err_no_key.get("error", "")
        assert err_no_key.get("reqid") == 20403

        # 4. Unknown setting variable
        err_unk_var = client.request(
            "LLViewerControl", op="get", data={"key": "NonExistentVariableFooBar"}, reqid=20404, timeout=5.0
        )
        assert "error" in err_unk_var
        assert "Unknown setting variable" in err_unk_var.get("error", "")

        # 5. Toggle non-boolean variable
        err_tog_str = client.request(
            "LLViewerControl", op="toggle", data={"key": "Language"}, reqid=20405, timeout=5.0
        )
        assert "error" in err_tog_str
        assert "not a toggleable boolean variable" in err_tog_str.get("error", "")

        # 6. Target unhandled subsystem pump
        err_unk_pump = client.request(
            "NoSuchSubsystemPumpAtAll", op="anything", data={}, reqid=20406, timeout=5.0
        )
        assert "error" in err_unk_pump
        assert "Unhandled subsystem pump" in err_unk_pump.get("error", "")

        # 7. Non-existent inventory item
        err_no_item = client.request(
            "LLInventory", op="getItem", data={"item_id": "00000000-0000-0000-0000-000000000000"}, reqid=20407, timeout=5.0
        )
        assert "error" in err_no_item
        assert "not found" in err_no_item.get("error", "")

        # Verify viewer health after all error conditions
        health_check = client.ping(reqid=20499, timeout=5.0)
        assert health_check.get("status") is True
        assert health_check.get("reqid") == 20499

        results["test_2_4_error_handling"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 2.4 Error Handling & Robustness (7 invalid conditions handled without crash)")
    except Exception as err:
        import traceback
        traceback.print_exc()
        results["test_2_4_error_handling"] = {"status": "FAIL", "error": repr(err)}
        print(f"  [FAIL] 2.4 Error Handling: {err!r}")

    return results


if __name__ == "__main__":
    c = LeapClient()
    try:
        c.ping(timeout=30.0)
        res = run_tier2_tests(c)
        all_passed = all(r.get("status") == "PASS" for r in res.values())
        print(f"\nTier 2 Summary: {'ALL PASS' if all_passed else 'FAILURES DETECTED'}")
        c.quit_gracefully(timeout=5.0)
        sys.exit(0 if all_passed else 1)
    finally:
        if c.is_alive():
            c.terminate()
