"""
High-Level LEAP Protocol Client for Aurora-Viewer Opaque-Box Testing.

Manages child process lifecycle, length-prefixed framing I/O over stdio pipes,
reqid request-response correlation, and asynchronous pub/sub event collection.
"""

import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

sys.path.insert(0, str(Path(__file__).parent))
from leap_codec import FrameDecoder, encode_frame, encode_message
from llsd_notation import format_notation, parse_notation


class LeapTimeoutError(Exception):
    """Raised when awaiting a LEAP response or event exceeds timeout."""
    pass


class LeapClient:
    """Opaque-box test client communicating with Aurora-Viewer over LEAP stdio."""

    def __init__(
        self,
        exe_path: Optional[str] = None,
        extra_args: Optional[List[str]] = None,
        cwd: Optional[str] = None,
        startup_timeout: float = 10.0,
    ):
        if exe_path is None:
            # Default to target/debug/aurora-viewer.exe
            candidate_paths = [
                Path("target/debug/aurora-viewer.exe"),
                Path("../target/debug/aurora-viewer.exe"),
                Path("Aurora-Viewer/target/debug/aurora-viewer.exe"),
                Path(__file__).parent.parent.parent / "target" / "debug" / "aurora-viewer.exe",
            ]
            for p in candidate_paths:
                if p.is_file():
                    exe_path = str(p.resolve())
                    break
            if exe_path is None:
                raise FileNotFoundError("Could not locate aurora-viewer.exe")

        self.exe_path = str(Path(exe_path).resolve())
        self.cwd = cwd or str(Path(self.exe_path).parent.parent.parent)
        self.extra_args = extra_args or []

        self.proc: Optional[subprocess.Popen] = None
        self.cmd_uuid: Optional[str] = None
        self.reply_uuid: Optional[str] = None
        self.features: Dict[str, Any] = {}
        self.intro_packet: Optional[Dict[str, Any]] = None

        self._decoder = FrameDecoder()
        self._reader_thread: Optional[threading.Thread] = None
        self._running = False

        self._pending_replies: Dict[Any, queue.Queue] = {}
        self._pending_lock = threading.Lock()

        self._event_queues: Dict[str, queue.Queue] = {}
        self._all_events: List[Dict[str, Any]] = []
        self._event_lock = threading.Lock()

        self._reqid_counter = 1000
        self._reqid_lock = threading.Lock()

        self._launch_and_handshake(timeout=startup_timeout)

    def _next_reqid(self) -> int:
        with self._reqid_lock:
            self._reqid_counter += 1
            return self._reqid_counter

    def _launch_and_handshake(self, timeout: float) -> None:
        cmd = [self.exe_path, "--leap-stdio"] + self.extra_args
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=self.cwd,
            bufsize=0,
        )

        # Attach process to Windows Job Object so child is killed if Python exits
        self._job = None
        if sys.platform == "win32":
            try:
                import ctypes
                from ctypes import wintypes
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                job = kernel32.CreateJobObjectW(None, None)
                if job:
                    class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
                        _fields_ = [
                            ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
                            ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
                            ("LimitFlags", wintypes.DWORD),
                            ("MinimumWorkingSetSize", ctypes.c_size_t),
                            ("MaximumWorkingSetSize", ctypes.c_size_t),
                            ("ActiveProcessLimit", wintypes.DWORD),
                            ("Affinity", ctypes.c_size_t),
                            ("PriorityClass", wintypes.DWORD),
                            ("SchedulingClass", wintypes.DWORD),
                        ]
                    class IO_COUNTERS(ctypes.Structure):
                        _fields_ = [
                            ("ReadOperationCount", ctypes.c_uint64),
                            ("WriteOperationCount", ctypes.c_uint64),
                            ("OtherOperationCount", ctypes.c_uint64),
                            ("ReadTransferCount", ctypes.c_uint64),
                            ("WriteTransferCount", ctypes.c_uint64),
                            ("OtherTransferCount", ctypes.c_uint64),
                        ]
                    class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
                        _fields_ = [
                            ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                            ("IoCounters", IO_COUNTERS),
                            ("ProcessMemoryLimit", ctypes.c_size_t),
                            ("JobMemoryLimit", ctypes.c_size_t),
                            ("PeakProcessMemoryLimit", ctypes.c_size_t),
                            ("PeakJobMemoryLimit", ctypes.c_size_t),
                        ]
                    info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
                    info.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                    kernel32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info))
                    kernel32.AssignProcessToJobObject(job, int(self.proc._handle))
                    self._job = job
            except Exception:
                pass

        self._running = True
        self.stderr_lines: List[str] = []

        self._reader_thread = threading.Thread(
            target=self._reader_loop, name="leap-reader", daemon=True
        )
        self._reader_thread.start()

        self._stderr_thread = threading.Thread(
            target=self._stderr_loop, name="leap-stderr", daemon=True
        )
        self._stderr_thread.start()

        # Await intro handshake packet
        t0 = time.time()
        while time.time() - t0 < timeout:
            with self._event_lock:
                if self.intro_packet is not None:
                    break
            time.sleep(0.02)

        if self.intro_packet is None:
            self.terminate()
            raise LeapTimeoutError("Timed out waiting for LEAP initial handshake intro frame")

    def _stderr_loop(self) -> None:
        """Background thread constantly draining child stderr to prevent pipe buffer deadlock."""
        try:
            while self._running and self.proc and self.proc.stderr:
                line = self.proc.stderr.readline()
                if not line:
                    break
                decoded = line.decode("utf-8", errors="replace")
                self.stderr_lines.append(decoded)
        except Exception:
            pass

    def _reader_loop(self) -> None:
        """Background thread reading stdout and decoding frames."""
        try:
            while self._running and self.proc and self.proc.stdout:
                chunk = self.proc.stdout.read(4096)
                if not chunk:
                    break
                self._decoder.feed(chunk)
                while self._decoder.has_frame():
                    try:
                        msg = self._decoder.pop_message()
                        self._dispatch_inbound(msg)
                    except Exception as err:
                        sys.stderr.write(f"Error parsing LEAP inbound frame: {err}\n")
        except Exception as err:
            if self._running:
                sys.stderr.write(f"LEAP reader error: {err}\n")
        finally:
            self._running = False

    def _dispatch_inbound(self, msg: Dict[str, Any]) -> None:
        pump = msg.get("pump", "")
        data = msg.get("data", {})

        with self._event_lock:
            self._all_events.append(msg)
            # Record initial intro handshake
            if self.intro_packet is None and isinstance(data, dict) and "command" in data:
                self.intro_packet = msg
                self.cmd_uuid = data.get("command")
                self.reply_uuid = pump
                self.features = data.get("features", {})

            # Enqueue into pump event queue
            if pump not in self._event_queues:
                self._event_queues[pump] = queue.Queue()
            self._event_queues[pump].put(msg)

        reqid = data.get("reqid") if isinstance(data, dict) else None
        if reqid is not None:
            with self._pending_lock:
                # Key comparison matching str or int or type
                q = None
                try:
                    q = self._pending_replies.get(reqid)
                except TypeError:
                    pass
                if q is None:
                    q = self._pending_replies.get(str(reqid))
                if q is None:
                    q = self._pending_replies.get(repr(reqid))
                if q is not None:
                    q.put(data)

    def send_raw(self, raw_bytes: bytes) -> None:
        """Send raw bytes directly to stdin (for framing boundary testing)."""
        if not self.proc or not self.proc.stdin:
            raise BrokenPipeError("Viewer process stdin is closed")
        self.proc.stdin.write(raw_bytes)
        self.proc.stdin.flush()

    def send_message(self, pump: str, data: Dict[str, Any]) -> None:
        """Encode and transmit a LEAP message frame."""
        frame = encode_message(pump, data)
        self.send_raw(frame)

    def request(
        self,
        pump: str,
        op: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        reqid: Any = None,
        timeout: float = 5.0,
    ) -> Dict[str, Any]:
        """
        Send a request expecting a reply correlated by reqid.
        Defaults destination reply pump to self.reply_uuid.
        """
        if reqid is None:
            reqid = self._next_reqid()

        payload: Dict[str, Any] = {}
        if op is not None:
            payload["op"] = op
        if data:
            payload.update(data)

        payload["reply"] = self.reply_uuid
        payload["reqid"] = reqid

        reply_q = queue.Queue()
        with self._pending_lock:
            try:
                self._pending_replies[reqid] = reply_q
            except TypeError:
                pass
            self._pending_replies[str(reqid)] = reply_q
            self._pending_replies[repr(reqid)] = reply_q

        try:
            self.send_message(pump, payload)
            reply = reply_q.get(timeout=timeout)
            return reply
        except queue.Empty:
            raise LeapTimeoutError(
                f"Timed out after {timeout}s waiting for reply to {pump}.{op} (reqid={reqid})"
            )
        finally:
            with self._pending_lock:
                try:
                    self._pending_replies.pop(reqid, None)
                except TypeError:
                    pass
                self._pending_replies.pop(str(reqid), None)
                self._pending_replies.pop(repr(reqid), None)

    def command(
        self,
        op: str,
        data: Optional[Dict[str, Any]] = None,
        reqid: Any = None,
        timeout: float = 5.0,
    ) -> Dict[str, Any]:
        """Issue an operation on the dedicated LEAP command pump."""
        if not self.cmd_uuid:
            raise RuntimeError("Command pump UUID not yet established via handshake")
        return self.request(self.cmd_uuid, op=op, data=data, reqid=reqid, timeout=timeout)

    def ping(self, reqid: Any = None, timeout: float = 5.0) -> Dict[str, Any]:
        """Send core ping request to command pump."""
        return self.command("ping", reqid=reqid, timeout=timeout)

    def listen(self, source: str, listener_name: Optional[str] = None, timeout: float = 5.0) -> bool:
        """Subscribe to an event pump."""
        listener = listener_name or f"test-listener-{self._next_reqid()}"
        res = self.command("listen", {"source": source, "listener": listener}, timeout=timeout)
        return bool(res.get("status", False))

    def stop_listening(self, source: str, listener_name: str, timeout: float = 5.0) -> bool:
        """Unsubscribe from an event pump."""
        res = self.command("stoplistening", {"source": source, "listener": listener_name}, timeout=timeout)
        return bool(res.get("status", False))

    def recv_event(self, pump: str, timeout: float = 5.0) -> Dict[str, Any]:
        """Receive an asynchronous event packet emitted on a pump."""
        t0 = time.time()
        while time.time() - t0 < timeout:
            with self._event_lock:
                q = self._event_queues.get(pump)
                if q and not q.empty():
                    return q.get_nowait()
            time.sleep(0.02)
        raise LeapTimeoutError(f"Timed out after {timeout}s waiting for event on pump '{pump}'")

    def drain_events(self, pump: str) -> List[Dict[str, Any]]:
        """Drain all currently queued events for a pump."""
        results = []
        with self._event_lock:
            q = self._event_queues.get(pump)
            if q:
                while not q.empty():
                    try:
                        results.append(q.get_nowait())
                    except queue.Empty:
                        break
        return results

    def _close_pipes(self) -> None:
        if self.proc:
            for p in (self.proc.stdin, self.proc.stdout, self.proc.stderr):
                if p:
                    try:
                        p.close()
                    except Exception:
                        pass

    def quit_gracefully(self, timeout: float = 5.0) -> int:
        """Request viewer shutdown via LLAppViewer.requestQuit and wait for exit."""
        try:
            self.request("LLAppViewer", op="requestQuit", timeout=2.0)
        except Exception:
            pass

        if self.proc:
            try:
                ret = self.proc.wait(timeout=timeout)
                self._running = False
                self._close_pipes()
                return ret
            except subprocess.TimeoutExpired:
                self.terminate()
                return self.proc.returncode if self.proc else -1
        return 0

    def terminate(self) -> None:
        """Force terminate child process."""
        self._running = False
        if self.proc:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=2.0)
            except Exception:
                try:
                    self.proc.kill()
                    self.proc.wait(timeout=1.0)
                except Exception:
                    pass
            self._close_pipes()

    def __del__(self) -> None:
        if self.is_alive():
            self.terminate()

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None
