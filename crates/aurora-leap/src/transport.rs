//! LEAP Transport layer.
//!
//! Manages subprocess lifecycle, standard I/O pipes, and dedicated background OS threads
//! (`leap-reader`, `leap-writer`, and `leap-stderr`) communicating via crossbeam channels.

use crossbeam_channel::{Receiver, Sender};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::codec::{CodecError, LeapDecoder, LeapMessage};

/// Internal transport events emitted by the reader thread.
#[derive(Debug, Clone, PartialEq)]
pub enum TransportEvent {
    /// Parsed message from external process.
    Message(LeapMessage),
    /// External process reached EOF (pipe closed normally).
    Disconnected,
    /// Protocol or framing violation detected on stream.
    ProtocolError(String),
}

/// Status of the transport session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportStatus {
    Running,
    Disconnected,
    Error,
}

/// Parse a command-line string into executable and argument tokens,
/// respecting single/double quotes and backslash escapes.
pub fn parse_command_line(cmd: &str) -> Result<(String, Vec<String>), String> {
    let trimmed = cmd.trim();
    if trimmed.is_empty() {
        return Err("empty command string".to_string());
    }

    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escape = false;

    for c in trimmed.chars() {
        if escape {
            current.push(c);
            escape = false;
            continue;
        }

        match c {
            '\\' if !in_single_quote => escape = true,
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            c if c.is_whitespace() && !in_single_quote && !in_double_quote => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    if tokens.is_empty() {
        return Err("no command executable found".to_string());
    }

    let executable = tokens.remove(0);
    Ok((executable, tokens))
}

/// RAII wrapper around a spawned child process to guarantee graceful termination.
pub struct ChildGuard {
    child: Option<Child>,
    desc: String,
}

impl ChildGuard {
    pub fn new(child: Child, desc: String) -> Self {
        Self {
            child: Some(child),
            desc,
        }
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// Check if child has exited without blocking.
    pub fn try_wait(&mut self) -> Result<Option<std::process::ExitStatus>, std::io::Error> {
        if let Some(child) = &mut self.child {
            let status = child.try_wait()?;
            if status.is_some() {
                self.child = None;
            }
            Ok(status)
        } else {
            Ok(None)
        }
    }

    /// Graceful termination: poll with grace period, kill if lingered, reap exit status.
    pub fn terminate(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };

        let pid = child.id();
        log::debug!(target: "leap", "{}: terminating child process (pid: {pid})", self.desc);

        // Grace period polling
        let start = Instant::now();
        let grace = Duration::from_millis(300);
        let mut exited = false;

        while start.elapsed() < grace {
            match child.try_wait() {
                Ok(Some(status)) => {
                    log::info!(target: "leap", "{}: child exited cleanly with status: {status}", self.desc);
                    exited = true;
                    break;
                }
                Ok(None) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    log::warn!(target: "leap", "{}: error polling child status: {e}", self.desc);
                    break;
                }
            }
        }

        // Force kill if lingered
        if !exited {
            log::warn!(target: "leap", "{}: child did not exit within grace period; sending kill signal", self.desc);
            let _ = child.kill();
        }

        // Reap exit status
        match child.wait() {
            Ok(status) => log::info!(target: "leap", "{}: child reaped with status: {status}", self.desc),
            Err(e) => log::error!(target: "leap", "{}: failed to reap child process: {e}", self.desc),
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

/// Spawn the reader thread (`leap-reader`) reading from an input stream.
pub fn spawn_reader<R: Read + Send + 'static>(
    mut reader: R,
    tx: Sender<TransportEvent>,
    desc: String,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("leap-reader".into())
        .spawn(move || {
            log::debug!(target: "leap", "{}: leap-reader thread started", desc);
            let mut decoder = LeapDecoder::default();
            let mut chunk = [0u8; 8192];

            loop {
                match reader.read(&mut chunk) {
                    Ok(0) => {
                        log::info!(target: "leap", "{}: pipe reached EOF", desc);
                        if decoder.has_unconsumed_data() {
                            log::warn!(
                                target: "leap",
                                "{}: EOF reached with unparsed bytes remaining: {:?}",
                                desc,
                                decoder.unconsumed_preview(40)
                            );
                        }
                        let _ = tx.send(TransportEvent::Disconnected);
                        break;
                    }
                    Ok(n) => {
                        decoder.push_bytes(&chunk[..n]);

                        // Drain all complete frames available in buffer
                        loop {
                            match decoder.decode_next() {
                                Ok(Some(msg)) => {
                                    if tx.send(TransportEvent::Message(msg)).is_err() {
                                        return;
                                    }
                                }
                                Ok(None) => break,
                                Err(CodecError::UnexpectedPrefixChar(c)) => {
                                    let err = format!("unexpected prefix character '{c:?}'");
                                    log::warn!(target: "leap", "{}: protocol violation: {err}", desc);
                                    let _ = tx.send(TransportEvent::ProtocolError(err));
                                    return;
                                }
                                Err(err) => {
                                    let err_str = err.to_string();
                                    log::warn!(target: "leap", "{}: framing decode error: {err_str}", desc);
                                    let _ = tx.send(TransportEvent::ProtocolError(err_str));
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        log::warn!(target: "leap", "{}: pipe read error: {e}", desc);
                        let _ = tx.send(TransportEvent::Disconnected);
                        break;
                    }
                }
            }
            log::debug!(target: "leap", "{}: leap-reader thread finished", desc);
        })
        .expect("failed to spawn leap-reader thread")
}

/// Spawn the writer thread (`leap-writer`) writing to an output stream.
pub fn spawn_writer<W: Write + Send + 'static>(
    mut writer: W,
    rx: Receiver<Vec<u8>>,
    desc: String,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("leap-writer".into())
        .spawn(move || {
            log::debug!(target: "leap", "{}: leap-writer thread started", desc);
            while let Ok(frame_bytes) = rx.recv() {
                if let Err(e) = writer.write_all(&frame_bytes) {
                    log::warn!(target: "leap", "{}: pipe write failed: {e}", desc);
                    break;
                }
                if let Err(e) = writer.flush() {
                    log::warn!(target: "leap", "{}: pipe flush failed: {e}", desc);
                    break;
                }
            }
            log::debug!(target: "leap", "{}: leap-writer thread finished", desc);
        })
        .expect("failed to spawn leap-writer thread")
}

/// Spawn the stderr logger thread (`leap-stderr`) reading lines from child stderr.
pub fn spawn_stderr<R: Read + Send + 'static>(reader: R, desc: String) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("leap-stderr".into())
        .spawn(move || {
            let mut buf_reader = BufReader::new(reader);
            let mut line = String::new();

            while let Ok(n) = buf_reader.read_line(&mut line) {
                if n == 0 {
                    break;
                }
                let trimmed = line.trim_end();
                if !trimmed.is_empty() {
                    log::warn!(target: "leap", "[{desc} stderr] {trimmed}");
                }
                line.clear();
            }
            log::debug!(target: "leap", "{}: leap-stderr thread finished", desc);
        })
        .expect("failed to spawn leap-stderr thread")
}

/// Subprocess transport holding process handle, threads, and queues.
pub struct SubprocessTransport {
    pub child_guard: ChildGuard,
    pub reader_handle: JoinHandle<()>,
    pub writer_handle: JoinHandle<()>,
    pub stderr_handle: JoinHandle<()>,
    pub inbound_rx: Receiver<TransportEvent>,
    pub outbound_tx: Sender<Vec<u8>>,
}

impl SubprocessTransport {
    pub fn spawn(command_line: &str) -> Result<Self, String> {
        let (exe, args) = parse_command_line(command_line)?;
        let desc = std::path::Path::new(&exe)
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "leap-child".into());

        let mut cmd = Command::new(&exe);
        cmd.args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            cmd.creation_flags(CREATE_NO_WINDOW);
        }

        let mut child = cmd.spawn().map_err(|e| {
            format!("failed to spawn child process '{command_line}': {e}")
        })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "failed to capture child stdin pipe".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "failed to capture child stdout pipe".to_string())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "failed to capture child stderr pipe".to_string())?;

        let (inbound_tx, inbound_rx) = crossbeam_channel::unbounded();
        let (outbound_tx, outbound_rx) = crossbeam_channel::unbounded();

        let reader_handle = spawn_reader(stdout, inbound_tx, desc.clone());
        let writer_handle = spawn_writer(stdin, outbound_rx, desc.clone());
        let stderr_handle = spawn_stderr(stderr, desc.clone());
        let child_guard = ChildGuard::new(child, desc);

        Ok(Self {
            child_guard,
            reader_handle,
            writer_handle,
            stderr_handle,
            inbound_rx,
            outbound_tx,
        })
    }
}

/// Direct standard I/O transport holding viewer stdio handles.
pub struct StdioTransport {
    pub reader_handle: JoinHandle<()>,
    pub writer_handle: JoinHandle<()>,
    pub inbound_rx: Receiver<TransportEvent>,
    pub outbound_tx: Sender<Vec<u8>>,
}

impl StdioTransport {
    pub fn attach() -> Result<Self, String> {
        let desc = "stdio-leap".to_string();
        let stdin = std::io::stdin();
        let stdout = std::io::stdout();

        let (inbound_tx, inbound_rx) = crossbeam_channel::unbounded();
        let (outbound_tx, outbound_rx) = crossbeam_channel::unbounded();

        let reader_handle = spawn_reader(stdin, inbound_tx, desc.clone());
        let writer_handle = spawn_writer(stdout, outbound_rx, desc);

        Ok(Self {
            reader_handle,
            writer_handle,
            inbound_rx,
            outbound_tx,
        })
    }
}
