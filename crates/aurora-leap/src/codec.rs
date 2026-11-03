//! Length-prefixed LLSD stream framing codec for the LEAP protocol.
//!
//! Framing format: `<byte_count>:<serialized_llsd_bytes>`
//!
//! Provides stateful chunk accumulation, non-numeric prefix rejection,
//! payload size limits, and binary auto-detection.

use aurora_llsd::{from_binary, from_notation, to_notation, Llsd, LlsdError, Map};

/// Maximum allowable length of the ASCII decimal prefix (e.g. 20 digits for u64::MAX).
pub const MAX_PREFIX_LEN: usize = 20;

/// Default maximum allowed payload size (16 MB).
pub const DEFAULT_MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

/// ASCII colon delimiter (`:`).
pub const COLON: u8 = b':';

/// Errors produced during LEAP message encoding or decoding.
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum CodecError {
    #[error("invalid length prefix: {0}")]
    InvalidLengthPrefix(String),

    #[error("non-numeric length prefix: '{0}'")]
    NonNumericPrefix(String),

    #[error("length prefix exceeded maximum limit of {max} bytes")]
    PrefixTooLong { max: usize },

    #[error("payload length {length} bytes exceeds maximum limit of {max} bytes")]
    PayloadTooLarge { length: usize, max: usize },

    #[error("invalid protocol: unexpected character '{0:?}' before colon")]
    UnexpectedPrefixChar(char),

    #[error("llsd parse failure: {0}")]
    Llsd(String),

    #[error("invalid leap envelope: {0}")]
    InvalidEnvelope(&'static str),

    #[error("io error: {0}")]
    Io(String),

    #[error("protocol error: {0}")]
    Protocol(String),
}

impl From<LlsdError> for CodecError {
    fn from(err: LlsdError) -> Self {
        CodecError::Llsd(err.to_string())
    }
}

/// A structured LEAP wire message consisting of a destination/source pump and an LLSD payload.
#[derive(Debug, Clone, PartialEq)]
pub struct LeapMessage {
    pub pump: String,
    pub data: Llsd,
}

impl LeapMessage {
    /// Create a new LEAP message envelope.
    pub fn new(pump: impl Into<String>, data: impl Into<Llsd>) -> Self {
        Self {
            pump: pump.into(),
            data: data.into(),
        }
    }

    /// Convert an LLSD Map into a validated LeapMessage envelope.
    /// Requires top-level Map with a string "pump" and a "data" element.
    pub fn from_llsd(llsd: Llsd) -> Result<Self, CodecError> {
        match llsd {
            Llsd::Map(mut map) => {
                let pump = match map.remove("pump") {
                    Some(Llsd::String(s)) => s,
                    _ => return Err(CodecError::InvalidEnvelope("missing 'pump' or 'data'")),
                };
                let data = match map.remove("data") {
                    Some(data) => data,
                    _ => return Err(CodecError::InvalidEnvelope("missing 'pump' or 'data'")),
                };
                Ok(Self { pump, data })
            }
            _ => Err(CodecError::InvalidEnvelope("missing 'pump' or 'data'")),
        }
    }

    /// Serialize this message envelope into an LLSD Map.
    pub fn to_llsd(&self) -> Llsd {
        let mut map = Map::new();
        map.insert("pump".to_string(), Llsd::String(self.pump.clone()));
        map.insert("data".to_string(), self.data.clone());
        Llsd::Map(map)
    }
}

/// Encoder for LEAP length-prefixed frames.
pub struct LeapEncoder;

impl LeapEncoder {
    /// Encode a LeapMessage into length-prefixed LLSD notation bytes.
    pub fn encode(msg: &LeapMessage) -> Vec<u8> {
        Self::encode_llsd(&msg.to_llsd())
    }

    /// Encode an explicit pump and payload into length-prefixed LLSD notation bytes.
    pub fn encode_message(pump: &str, data: &Llsd) -> Vec<u8> {
        let msg = LeapMessage::new(pump, data.clone());
        Self::encode(&msg)
    }

    /// Encode an arbitrary LLSD value into `<len>:<notation_bytes>`.
    pub fn encode_llsd(llsd: &Llsd) -> Vec<u8> {
        let notation = to_notation(llsd);
        let payload = notation.as_bytes();
        let len_str = payload.len().to_string();

        let mut out = Vec::with_capacity(len_str.len() + 1 + payload.len());
        out.extend_from_slice(len_str.as_bytes());
        out.push(COLON);
        out.extend_from_slice(payload);
        out
    }
}

/// Stateful stream decoder for LEAP length-prefixed frames.
#[derive(Debug, Clone)]
pub struct LeapDecoder {
    buffer: Vec<u8>,
    max_payload_size: usize,
    max_prefix_len: usize,
}

impl Default for LeapDecoder {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            max_payload_size: DEFAULT_MAX_PAYLOAD_SIZE,
            max_prefix_len: MAX_PREFIX_LEN,
        }
    }
}

impl LeapDecoder {
    /// Create a new decoder with custom payload and prefix limits.
    pub fn new(max_payload_size: usize, max_prefix_len: usize) -> Self {
        Self {
            buffer: Vec::new(),
            max_payload_size,
            max_prefix_len,
        }
    }

    /// Push raw bytes received from a stream into the internal accumulator.
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    /// Check if the internal buffer has unconsumed bytes.
    pub fn has_unconsumed_data(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// Return a human-readable preview of unconsumed buffer data.
    pub fn unconsumed_preview(&self, max_len: usize) -> String {
        let preview_len = self.buffer.len().min(max_len);
        String::from_utf8_lossy(&self.buffer[..preview_len]).into_owned()
    }

    /// Decode the next complete LeapMessage from the internal buffer.
    pub fn decode_next(&mut self) -> Result<Option<LeapMessage>, CodecError> {
        let llsd_opt = self.decode_llsd_internal()?;
        match llsd_opt {
            Some(llsd) => {
                let msg = LeapMessage::from_llsd(llsd)?;
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    /// Decode the next raw LLSD value from the internal buffer.
    pub fn decode_llsd_next(&mut self) -> Result<Option<Llsd>, CodecError> {
        self.decode_llsd_internal()
    }

    /// Decode directly from an external `Vec<u8>` buffer, consuming bytes as frames are decoded.
    pub fn decode(&mut self, src: &mut Vec<u8>) -> Result<Option<LeapMessage>, CodecError> {
        if !self.buffer.is_empty() {
            self.buffer.extend_from_slice(src);
            src.clear();
            self.decode_next()
        } else {
            let llsd_opt = Self::decode_llsd_from_buf(src, self.max_payload_size, self.max_prefix_len)?;
            match llsd_opt {
                Some(llsd) => {
                    let msg = LeapMessage::from_llsd(llsd)?;
                    Ok(Some(msg))
                }
                None => Ok(None),
            }
        }
    }

    fn decode_llsd_internal(&mut self) -> Result<Option<Llsd>, CodecError> {
        Self::decode_llsd_from_buf(&mut self.buffer, self.max_payload_size, self.max_prefix_len)
    }

    /// Static core parsing logic operating on a mutable byte buffer.
    fn decode_llsd_from_buf(
        buf: &mut Vec<u8>,
        max_payload_size: usize,
        max_prefix_len: usize,
    ) -> Result<Option<Llsd>, CodecError> {
        // Step 1: Scan for colon delimiter
        let colon_pos = buf.iter().position(|&b| b == COLON);

        match colon_pos {
            None => {
                // Check if a newline is present before any colon (e.g. raw terminal text)
                if let Some(&b) = buf.iter().find(|&&b| b == b'\n' || b == b'\r') {
                    return Err(CodecError::UnexpectedPrefixChar(b as char));
                }
                for &b in buf.iter() {
                    if !b.is_ascii_digit() {
                        return Err(CodecError::NonNumericPrefix(
                            String::from_utf8_lossy(buf).into_owned(),
                        ));
                    }
                }
                // Prefix length limit check
                if buf.len() >= max_prefix_len {
                    return Err(CodecError::PrefixTooLong {
                        max: max_prefix_len,
                    });
                }
                // Incomplete header, need more bytes
                Ok(None)
            }
            Some(colon_idx) => {
                // Header must not exceed max prefix length
                if colon_idx > max_prefix_len {
                    return Err(CodecError::PrefixTooLong {
                        max: max_prefix_len,
                    });
                }
                if colon_idx == 0 {
                    return Err(CodecError::InvalidLengthPrefix("empty prefix".to_string()));
                }

                // Verify all bytes prior to colon are ASCII digits
                let prefix_slice = &buf[..colon_idx];
                for &b in prefix_slice {
                    if !b.is_ascii_digit() {
                        let invalid_str = String::from_utf8_lossy(prefix_slice).into_owned();
                        return Err(CodecError::NonNumericPrefix(invalid_str));
                    }
                }

                // Parse decimal length
                let len_str = match std::str::from_utf8(prefix_slice) {
                    Ok(s) => s,
                    Err(_) => {
                        return Err(CodecError::NonNumericPrefix(
                            String::from_utf8_lossy(prefix_slice).into_owned(),
                        ))
                    }
                };
                let payload_len: usize = match len_str.parse() {
                    Ok(len) => len,
                    Err(_) => return Err(CodecError::InvalidLengthPrefix(len_str.to_string())),
                };

                // Guard against payload DoS
                if payload_len > max_payload_size {
                    return Err(CodecError::PayloadTooLarge {
                        length: payload_len,
                        max: max_payload_size,
                    });
                }

                // Step 2: Check if entire frame (header + colon + payload) has arrived
                let total_frame_len = colon_idx + 1 + payload_len;
                if buf.len() < total_frame_len {
                    // Frame payload incomplete, wait for more data
                    return Ok(None);
                }

                // Step 3: Extract payload slice
                let payload_slice = &buf[colon_idx + 1..total_frame_len];

                // Auto-detect binary vs notation
                let llsd = if payload_slice.starts_with(b"<? LLSD/Binary ?>") {
                    let (val, _consumed) = from_binary(payload_slice)?;
                    val
                } else {
                    from_notation(payload_slice)?
                };

                // Consume frame bytes from buffer
                buf.drain(..total_frame_len);

                Ok(Some(llsd))
            }
        }
    }
}

// Type aliases for naming compatibility
pub type FrameDecoder = LeapDecoder;
pub type FrameEncoder = LeapEncoder;
pub type FrameError = CodecError;
