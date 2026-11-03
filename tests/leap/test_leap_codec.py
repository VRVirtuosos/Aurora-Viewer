"""Unit tests for leap_codec.py."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from leap_codec import FrameDecoder, FrameError, encode_frame, encode_message


def test_codec_single_frame():
    data = {"pump": "test_pump", "data": {"msg": "hello", "count": 10}}
    encoded = encode_frame(data)
    decoder = FrameDecoder()
    decoder.feed(encoded)
    assert decoder.has_frame()
    msg = decoder.pop_message()
    assert msg["pump"] == "test_pump"
    assert msg["data"]["msg"] == "hello"
    assert msg["data"]["count"] == 10
    assert not decoder.has_frame()


def test_codec_pipelining():
    frames = [
        encode_message("pump_a", {"id": i})
        for i in range(50)
    ]
    stream = b"".join(frames)
    decoder = FrameDecoder()
    decoder.feed(stream)

    for i in range(50):
        assert decoder.has_frame()
        msg = decoder.pop_message()
        assert msg["pump"] == "pump_a"
        assert msg["data"]["id"] == i
    assert not decoder.has_frame()


def test_codec_byte_by_byte():
    msg = {"pump": "stream", "data": {"text": "chunked boundary test"}}
    encoded = encode_frame(msg)
    decoder = FrameDecoder()

    for byte in encoded:
        assert not decoder.has_frame() or byte == encoded[-1]
        decoder.feed(bytes([byte]))

    assert decoder.has_frame()
    popped = decoder.pop_message()
    assert popped["data"]["text"] == "chunked boundary test"


def test_codec_prime_chunks():
    msgs = [{"pump": f"p_{i}", "data": {"val": i * 100}} for i in range(10)]
    stream = b"".join(encode_frame(m) for m in msgs)
    decoder = FrameDecoder()

    chunk_size = 7
    for offset in range(0, len(stream), chunk_size):
        chunk = stream[offset : offset + chunk_size]
        decoder.feed(chunk)

    for i in range(10):
        assert decoder.has_frame()
        m = decoder.pop_message()
        assert m["pump"] == f"p_{i}"
        assert m["data"]["val"] == i * 100
    assert not decoder.has_frame()


def test_codec_invalid_prefix():
    decoder = FrameDecoder()
    try:
        decoder.feed(b"abc:payload")
        assert False, "Should have raised FrameError for non-numeric prefix"
    except FrameError:
        pass


def test_codec_prefix_too_long():
    decoder = FrameDecoder(max_prefix_len=10)
    try:
        decoder.feed(b"12345678901234567890")
        assert False, "Should have raised FrameError for prefix exceeding limit"
    except FrameError:
        pass


if __name__ == "__main__":
    test_codec_single_frame()
    test_codec_pipelining()
    test_codec_byte_by_byte()
    test_codec_prime_chunks()
    test_codec_invalid_prefix()
    test_codec_prefix_too_long()
    print("ALL LEAP CODEC TESTS PASSED!")
