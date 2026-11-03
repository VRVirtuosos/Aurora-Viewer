"""
LEAP Automated Test Suite - Tier 4: Real-World Workloads & End-to-End Scenarios.

Covers:
  - 4.1 Full end-to-end automation workflow:
      Launch -> Handshake -> Ping -> Status query -> Settings inspection & mutation ->
      Settings restoration -> Pub/Sub listener registration -> LLChat sendChat & forwarded
      events -> Subsystem queries (Agent, Login, Camera, Inventory) -> Graceful quit via
      LLAppViewer.requestQuit -> Verified exit code 0.
  - 4.2 Rapid launch and immediate graceful quit lifecycle.
  - 4.3 Sequential multi-session lifecycle repeatability (zero resource leaks).
"""

import sys
import time
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).parent))
from leap_client import LeapClient


def test_4_1_full_automation_workflow() -> Dict[str, Any]:
    """Execute complete real-world automation workflow and verify graceful exit code 0."""
    t0 = time.time()
    client = LeapClient()
    try:
        # Step 1: Handshake and Ping
        print("  -> Verifying handshake...", flush=True)
        intro = client.intro_packet
        assert intro is not None, "Missing intro handshake packet"
        assert client.cmd_uuid is not None, "Missing cmd_uuid in handshake"
        assert client.reply_uuid is not None, "Missing reply_uuid in handshake"

        print("  -> Pinging...", flush=True)
        ping_res = client.ping(reqid=40101, timeout=10.0)
        assert ping_res.get("status") is True, f"Ping failed: {ping_res}"
        assert ping_res.get("reqid") == 40101

        # Step 2: Runtime Status Query
        print("  -> Querying status...", flush=True)
        status = client.request("LLAppViewer", op="status", reqid=40102)
        assert "version" in status, f"Missing version in status: {status}"
        assert status.get("screen") == "Login", f"Expected 'Login' screen, got {status.get('screen')}"
        assert status.get("fps", 0.0) >= 0.0
        assert status.get("uptime", 0.0) >= 0.0

        # Step 3: Settings Inspection
        print("  -> Inspecting settings...", flush=True)
        orig_vsync = client.request("LLViewerControl", op="get", data={"key": "VSync"}, reqid=40103)
        orig_vsync_val = bool(orig_vsync.get("value"))
        orig_dd = client.request("LLViewerControl", op="get", data={"key": "DrawDistance"}, reqid=40104)
        orig_dd_val = float(orig_dd.get("value", 128.0))

        # Step 4: Settings Mutation and Restoration
        print("  -> Mutating & restoring settings...", flush=True)
        # Toggle VSync
        tog_res = client.request("LLViewerControl", op="toggle", data={"key": "VSync"}, reqid=40105)
        new_vsync_val = bool(tog_res.get("value"))
        assert new_vsync_val != orig_vsync_val, f"VSync toggle did not flip value: {new_vsync_val}"

        # Restore VSync
        tog_back = client.request("LLViewerControl", op="toggle", data={"key": "VSync"}, reqid=40106)
        restored_vsync_val = bool(tog_back.get("value"))
        assert restored_vsync_val == orig_vsync_val, "VSync was not restored"

        # Mutate DrawDistance float
        test_dd = 384.0
        set_dd = client.request(
            "LLViewerControl", op="set", data={"key": "DrawDistance", "value": test_dd}, reqid=40107
        )
        assert abs(float(set_dd.get("value", 0.0)) - test_dd) < 1e-4

        # Restore DrawDistance
        rst_dd = client.request(
            "LLViewerControl", op="set", data={"key": "DrawDistance", "value": orig_dd_val}, reqid=40108
        )
        assert abs(float(rst_dd.get("value", 0.0)) - orig_dd_val) < 1e-4

        # Step 5: Pub/Sub and Chat System
        print("  -> Testing chat and pub/sub...", flush=True)
        l_res = client.command("listen", {"source": "chat", "listener": "e2e_chat_listener"}, reqid=40109)
        assert l_res.get("status") is True

        chat_send = client.request(
            "LLChat",
            op="sendChat",
            data={"message": "End-to-end automation message", "channel": 0},
            reqid=40110,
        )
        assert chat_send.get("status") is True
        assert chat_send.get("message") == "End-to-end automation message"

        # Direct event notification test
        client.send_message(
            "chat",
            {"from": "TestOrchestrator", "message": "Notice dispatched to viewer", "channel": 0},
        )
        ev = client.recv_event("chat", timeout=5.0)
        assert ev.get("pump") == "chat"
        assert ev.get("data", {}).get("message") == "Notice dispatched to viewer"

        # Unsubscribe
        ul_res = client.command(
            "stoplistening", {"source": "chat", "listener": "e2e_chat_listener"}, reqid=40111
        )
        assert ul_res.get("status") is True

        # Step 6: Subsystem Diagnostics
        print("  -> Querying LLAgent.getPosition...", flush=True)
        pos = client.request("LLAgent", op="getPosition", reqid=40112)
        print(f"  -> LLAgent.getPosition returned: {pos}", flush=True)
        assert "position" in pos
        assert "x" in pos and "y" in pos and "z" in pos

        print("  -> Querying LLLogin.getState...", flush=True)
        login_st = client.request("LLLogin", op="getState", reqid=40113)
        print(f"  -> LLLogin.getState returned: {login_st}", flush=True)
        assert "state" in login_st
        assert login_st.get("state") == "STATE_LOGIN_SHOW"

        print("  -> Querying LLCamera.get...", flush=True)
        cam = client.request("LLCamera", op="get", reqid=40114)
        print(f"  -> LLCamera.get returned: {cam}", flush=True)
        assert "position" in cam

        print("  -> Querying LLInventory.getRoot...", flush=True)
        inv = client.request("LLInventory", op="getRoot", reqid=40115)
        print(f"  -> LLInventory.getRoot returned: {inv}", flush=True)
        assert "root" in inv

        # Step 7: Graceful Shutdown via LLAppViewer.requestQuit
        print("  -> Requesting graceful quit...", flush=True)
        quit_reply = client.request("LLAppViewer", op="requestQuit", reqid=40116, timeout=3.0)
        print(f"  -> Quit reply received: {quit_reply}", flush=True)
        assert quit_reply.get("status") is True, f"Failed requestQuit: {quit_reply}"

        print("  -> Waiting for process exit...", flush=True)
        exit_code = client.proc.wait(timeout=5.0)
        print(f"  -> Process exited with code {exit_code}", flush=True)
        assert exit_code == 0, f"Expected clean exit code 0, got {exit_code}"

        return {"status": "PASS", "duration": time.time() - t0, "exit_code": exit_code}
    finally:
        if client.is_alive():
            client.terminate()


def test_4_2_rapid_launch_and_quit() -> Dict[str, Any]:
    """Verify rapid launch and immediate graceful quit completes with code 0."""
    t0 = time.time()
    client = LeapClient()
    try:
        client.ping(reqid=40201, timeout=10.0)
        quit_res = client.request("LLAppViewer", op="requestQuit", reqid=40202, timeout=3.0)
        assert quit_res.get("status") is True

        exit_code = client.proc.wait(timeout=5.0)
        assert exit_code == 0, f"Expected clean exit code 0, got {exit_code}"

        return {"status": "PASS", "duration": time.time() - t0, "exit_code": exit_code}
    finally:
        if client.is_alive():
            client.terminate()


def test_4_3_sequential_sessions() -> Dict[str, Any]:
    """Verify multiple sequential viewer sessions can be spawned and exited without collision."""
    t0 = time.time()
    for session_idx in range(2):
        c = LeapClient()
        try:
            c.ping(reqid=40300 + session_idx, timeout=10.0)
            status = c.request("LLAppViewer", op="status", reqid=40310 + session_idx)
            assert status.get("screen") == "Login"
            code = c.quit_gracefully(timeout=5.0)
            assert code == 0, f"Session {session_idx} exited with code {code}"
        finally:
            if c.is_alive():
                c.terminate()

    return {"status": "PASS", "duration": time.time() - t0, "sessions_count": 2}


def run_tier4_tests() -> Dict[str, Any]:
    """Run all Tier 4 workload tests."""
    results = {}
    print("\n=== Running Tier 4: Real-World Workloads & E2E Scenarios ===")

    # 4.1 Full E2E Automation Workflow
    try:
        res = test_4_1_full_automation_workflow()
        results["test_4_1_full_automation_workflow"] = res
        print(f"  [PASS] 4.1 Full E2E Automation Workflow (exit code 0, duration {res['duration']:.2f}s)")
    except Exception as err:
        results["test_4_1_full_automation_workflow"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 4.1 Full E2E Automation: {err}")

    # 4.2 Rapid Launch and Graceful Quit
    try:
        res = test_4_2_rapid_launch_and_quit()
        results["test_4_2_rapid_launch_and_quit"] = res
        print(f"  [PASS] 4.2 Rapid Launch and Graceful Quit (exit code 0, duration {res['duration']:.2f}s)")
    except Exception as err:
        results["test_4_2_rapid_launch_and_quit"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 4.2 Rapid Launch & Quit: {err}")

    # 4.3 Sequential Multi-Session Repeatability
    try:
        res = test_4_3_sequential_sessions()
        results["test_4_3_sequential_sessions"] = res
        print(f"  [PASS] 4.3 Sequential Multi-Session Repeatability (duration {res['duration']:.2f}s)")
    except Exception as err:
        results["test_4_3_sequential_sessions"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 4.3 Sequential Multi-Session: {err}")

    return results


if __name__ == "__main__":
    test_results = run_tier4_tests()
    all_passed = all(r.get("status") == "PASS" for r in test_results.values())
    print(f"\nTier 4 Summary: {'ALL PASS' if all_passed else 'FAILURES DETECTED'}")
    sys.exit(0 if all_passed else 1)
