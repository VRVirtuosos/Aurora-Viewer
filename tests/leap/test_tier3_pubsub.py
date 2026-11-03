"""
LEAP Automated Test Suite - Tier 3: Cross-Feature & Pub/Sub.

Covers:
  - Pub/sub listener lifecycle (listen, duplicate rejection, stoplistening).
  - Chat transmission via LLChat.sendChat and event notification on 'chat' pump.
  - High-throughput pipelining stress test (75 back-to-back interleaved requests).
"""

import queue
import sys
import time
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).parent))
from leap_client import LeapClient


def run_tier3_tests(client: LeapClient) -> Dict[str, Any]:
    """Run all Tier 3 tests against an active LeapClient instance."""
    results = {}
    print("\n=== Running Tier 3: Cross-Feature & Pub/Sub ===")

    # 3.1 Pub/Sub Listener Registration & Unregistration
    t0 = time.time()
    try:
        # Listen to StartupState
        l_startup = client.command(
            "listen", {"source": "StartupState", "listener": "test_startup_listener"}, reqid=30101
        )
        assert l_startup.get("status") is True, f"Failed listen on StartupState: {l_startup}"

        # Duplicate listener rejection on same pump
        l_dup = client.command(
            "listen", {"source": "StartupState", "listener": "test_startup_listener"}, reqid=30102
        )
        assert l_dup.get("status") is False, f"Duplicate listener was not rejected: {l_dup}"

        # Listen to chat
        l_chat = client.command(
            "listen", {"source": "chat", "listener": "test_chat_listener"}, reqid=30103
        )
        assert l_chat.get("status") is True, f"Failed listen on chat: {l_chat}"

        # Unregister from StartupState
        ul_startup = client.command(
            "stoplistening", {"source": "StartupState", "listener": "test_startup_listener"}, reqid=30104
        )
        assert ul_startup.get("status") is True, f"Failed stoplistening on StartupState: {ul_startup}"

        # Unregister non-existent listener
        ul_none = client.command(
            "stoplistening", {"source": "StartupState", "listener": "non_existent"}, reqid=30105
        )
        assert ul_none.get("status") is False, "Expected false when stopping non-existent listener"

        results["test_3_1_pubsub_lifecycle"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 3.1 Pub/Sub Listener Lifecycle (listen, duplicate rejection, stoplistening)")
    except Exception as err:
        results["test_3_1_pubsub_lifecycle"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 3.1 Pub/Sub Lifecycle: {err}")

    # 3.2 Chat Transmission & Event Notification
    t0 = time.time()
    try:
        # Re-verify listening to chat
        client.command("listen", {"source": "chat", "listener": "chat_event_monitor"}, reqid=30200)

        # 1. LLChat.sendChat subsystem command
        chat_reply = client.request(
            "LLChat",
            op="sendChat",
            data={"message": "Broadcasting automated test message", "channel": 0},
            reqid=30201,
        )
        assert chat_reply.get("status") is True
        assert chat_reply.get("message") == "Broadcasting automated test message"
        assert chat_reply.get("channel") == 0

        # 2. Emitted chat event forwarding across LEAP stream
        client.send_message(
            "chat",
            {
                "from": "AutomatedTestAgent",
                "message": "Direct pump notification event",
                "channel": 0,
                "type": 1,
            },
        )

        # Wait for forwarded event
        event = client.recv_event("chat", timeout=5.0)
        assert event.get("pump") == "chat", f"Expected pump 'chat', got {event.get('pump')}"
        ev_data = event.get("data", {})
        assert ev_data.get("from") == "AutomatedTestAgent"
        assert ev_data.get("message") == "Direct pump notification event"

        results["test_3_2_chat_events"] = {"status": "PASS", "duration": time.time() - t0}
        print("  [PASS] 3.2 Chat Transmission & Pub/Sub Event Notification")
    except Exception as err:
        results["test_3_2_chat_events"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 3.2 Chat Events: {err}")

    # 3.3 High-Throughput Pipelining Stress Test (75 Back-to-Back Requests)
    t0 = time.time()
    try:
        total_requests = 75
        req_configs = []
        pending_queues = {}

        for i in range(total_requests):
            reqid = 30300 + i
            q = queue.Queue()
            with client._pending_lock:
                client._pending_replies[reqid] = q
                client._pending_replies[str(reqid)] = q
            pending_queues[reqid] = q

            # Rotate through 3 request types: ping, setting query, status query
            req_type = i % 3
            if req_type == 0:
                payload = {"op": "ping", "reply": client.reply_uuid, "reqid": reqid}
                pump = client.cmd_uuid
            elif req_type == 1:
                key = ["DrawDistance", "VSync", "Shadows", "Language"][i % 4]
                payload = {"op": "get", "key": key, "reply": client.reply_uuid, "reqid": reqid}
                pump = "LLViewerControl"
            else:
                payload = {"op": "status", "reply": client.reply_uuid, "reqid": reqid}
                pump = "LLAppViewer"

            req_configs.append((pump, payload, reqid))

        t_send_start = time.time()
        for pump, payload, reqid in req_configs:
            client.send_message(pump, payload)
        send_duration = time.time() - t_send_start

        # Collect and correlate all 75 replies
        t_recv_start = time.time()
        for _, _, reqid in req_configs:
            reply = pending_queues[reqid].get(timeout=10.0)
            assert reply.get("reqid") == reqid, f"reqid correlation mismatch: {reply}"
        recv_duration = time.time() - t_recv_start

        results["test_3_3_pipelining"] = {
            "status": "PASS",
            "duration": time.time() - t0,
            "requests_count": total_requests,
            "send_duration_sec": send_duration,
            "recv_duration_sec": recv_duration,
        }
        print(
            f"  [PASS] 3.3 High-Throughput Pipelining ({total_requests} interleaved requests: "
            f"sent in {send_duration*1000:.1f}ms, collected in {recv_duration*1000:.1f}ms)"
        )
    except Exception as err:
        results["test_3_3_pipelining"] = {"status": "FAIL", "error": str(err)}
        print(f"  [FAIL] 3.3 Pipelining: {err}")

    return results


if __name__ == "__main__":
    c = LeapClient()
    try:
        c.ping(timeout=30.0)
        res = run_tier3_tests(c)
        all_passed = all(r.get("status") == "PASS" for r in res.values())
        print(f"\nTier 3 Summary: {'ALL PASS' if all_passed else 'FAILURES DETECTED'}")
        c.quit_gracefully(timeout=5.0)
        sys.exit(0 if all_passed else 1)
    finally:
        if c.is_alive():
            c.terminate()
