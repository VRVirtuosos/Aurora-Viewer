"""
LEAP Wire Protocol Framing Codec.

Encodes and decodes length-prefixed LLSD frames:
    <byte_count>:<serialized_llsd_bytes>

Handles stream chunking, partial reads, multiple frames per buffer,
and byte-by-byte boundary reassembly.
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from llsd_notation import format_notation, parse_notation


class FrameError(Exception):
    """Raised when framing protocol rules are violated."""
    pass


def encode_frame(payload: Union[bytes, str, Dict[str, Any], List[Any]]) -> bytes:
    """Format payload into framed bytes: <length>:<payload_bytes>."""
    if isinstance(payload, bytes):
        payload_bytes = payload
    elif isinstance(payload, str):
        payload_bytes = payload.encode("utf-8")
    else:
        notation_str = format_notation(payload)
        payload_bytes = notation_str.encode("utf-8")

    prefix = f"{len(payload_bytes)}:".encode("ascii")
    return prefix + payload_bytes


def encode_message(pump: str, data: Any) -> bytes:
    """Encode an envelope map {'pump': pump, 'data': data} into framed bytes."""
    envelope = {"pump": pump, "data": data}
    return encode_frame(envelope)


class FrameDecoder:
    """Stateful streaming decoder for LEAP framed byte streams."""

    STATE_PREFIX = 0
    STATE_PAYLOAD = 1

    def __init__(self, max_payload_size: int = 16 * 1024 * 1024, max_prefix_len: int = 20):
        self.buffer = bytearray()
        self.state = self.STATE_PREFIX
        self.expected_len = 0
        self.max_payload_size = max_payload_size
        self.max_prefix_len = max_prefix_len
        self.frames: List[bytes] = []

    def feed(self, chunk: bytes) -> None:
        """Feed bytes into the decoder buffer and process complete frames."""
        self.buffer.extend(chunk)
        self._process()

    def _process(self) -> None:
        while True:
            if self.state == self.STATE_PREFIX:
                colon_idx = self.buffer.find(b":")
                if colon_idx == -1:
                    # Check if prefix has grown too large without finding colon
                    if len(self.buffer) > self.max_prefix_len:
                        raise FrameError(
                            f"Prefix length exceeds limit ({len(self.buffer)} > {self.max_prefix_len}): "
                            f"{bytes(self.buffer[:30])!r}"
                        )
                    # Need more bytes to find ':'
                    break

                prefix_bytes = bytes(self.buffer[:colon_idx])
                if not prefix_bytes:
                    raise FrameError("Empty length prefix before ':'")

                # Validate ASCII decimal digits
                try:
                    prefix_str = prefix_bytes.decode("ascii")
                    self.expected_len = int(prefix_str)
                except (UnicodeDecodeError, ValueError) as err:
                    raise FrameError(f"Non-numeric length prefix '{prefix_bytes!r}': {err}")

                if self.expected_len < 0:
                    raise FrameError(f"Negative length prefix {self.expected_len}")

                if self.expected_len > self.max_payload_size:
                    raise FrameError(
                        f"Payload length {self.expected_len} exceeds max {self.max_payload_size}"
                    )

                # Remove prefix and colon
                del self.buffer[: colon_idx + 1]
                self.state = self.STATE_PAYLOAD

            if self.state == self.STATE_PAYLOAD:
                if len(self.buffer) < self.expected_len:
                    # Need more payload bytes
                    break

                payload = bytes(self.buffer[: self.expected_len])
                del self.buffer[: self.expected_len]
                self.frames.append(payload)
                self.state = self.STATE_PREFIX
                self.expected_len = 0

    def has_frame(self) -> bool:
        """Check if at least one complete frame is available."""
        return len(self.frames) > 0

    def pop_frame(self) -> bytes:
        """Pop the oldest complete raw frame."""
        if not self.frames:
            raise IndexError("No complete frame available")
        return self.frames.pop(0)

    def pop_message(self) -> Dict[str, Any]:
        """Pop and parse the oldest complete frame as an LLSD envelope map."""
        raw = self.pop_frame()
        parsed = parse_notation(raw)
        if not isinstance(parsed, dict):
            raise FrameError(f"Frame payload is not an LLSD Map: {type(parsed).__name__}")
        return parsed
