//! Spectator client for connecting to a remote LiveMesh broadcast relay.
//! Receives live mesh snapshots in a non-edit (view-only) capacity.

use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    io::{self, BufRead, BufReader, Read, Write},
    net::TcpStream,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;

use super::{broadcast::GUEST_MAGIC, protocol::{self, Prepared}};

#[derive(Clone, Debug, Default)]
pub struct SpectatorStatus {
    pub connected: bool,
    pub host: String,
    pub port: u16,
    pub host_agent_id: Option<Uuid>,
    pub received: u64,
    pub error: Option<String>,
}

#[derive(Default)]
pub struct SpectatorShared {
    pub stop: AtomicBool,
    pub status: Mutex<SpectatorStatus>,
    pub latest: Mutex<Option<Prepared>>,
    pub inworld_sync: Mutex<Option<super::broadcast::InworldSync>>,
}

pub struct Client {
    pub shared: Arc<SpectatorShared>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Release);
    }
}

pub fn start(
    host: String,
    port: u16,
    passcode: Option<String>,
    supported: HashSet<String>,
    ctx: egui::Context,
    pool: Option<Arc<aurora_render::StagingPool>>,
) -> io::Result<Client> {
    let shared = Arc::new(SpectatorShared::default());
    {
        let mut st = shared.status.lock();
        st.host = host.clone();
        st.port = port;
    }

    let worker = shared.clone();
    std::thread::Builder::new()
        .name("blender-spectator".into())
        .spawn(move || {
            run_spectator(host, port, passcode, supported, ctx, pool, worker);
        })?;

    Ok(Client { shared })
}

fn run_spectator(
    host: String,
    port: u16,
    passcode: Option<String>,
    supported: HashSet<String>,
    ctx: egui::Context,
    pool: Option<Arc<aurora_render::StagingPool>>,
    shared: Arc<SpectatorShared>,
) {
    let addr = format!("{host}:{port}");
    let Ok(mut stream) = TcpStream::connect(&addr) else {
        shared.status.lock().error = Some(format!("Could not connect to {addr}"));
        ctx.request_repaint();
        return;
    };

    if let Err(e) = stream.set_read_timeout(Some(Duration::from_millis(150))) {
        shared.status.lock().error = Some(e.to_string());
        ctx.request_repaint();
        return;
    }
    if let Err(e) = stream.set_write_timeout(Some(Duration::from_secs(4))) {
        shared.status.lock().error = Some(e.to_string());
        ctx.request_repaint();
        return;
    }
    let _ = stream.set_nodelay(true);

    // 1. Send Guest Magic
    if let Err(e) = stream.write_all(GUEST_MAGIC) {
        shared.status.lock().error = Some(e.to_string());
        ctx.request_repaint();
        return;
    }

    // 2. Send Auth Header Line
    let auth_payload = serde_json::json!({
        "passcode": passcode,
        "client_version": "0.1.0"
    });
    if let Err(e) = writeln!(stream, "{auth_payload}") {
        shared.status.lock().error = Some(e.to_string());
        ctx.request_repaint();
        return;
    }

    // 3. Read Greeting Header
    let mut reader = BufReader::new(stream.try_clone().unwrap_or_else(|_| stream));
    let mut greeting_line = String::new();
    let last_read = Instant::now();
    loop {
        if shared.stop.load(Ordering::Acquire) {
            return;
        }
        match reader.read_line(&mut greeting_line) {
            Ok(0) => {
                shared.status.lock().error = Some("Host disconnected before greeting".into());
                ctx.request_repaint();
                return;
            }
            Ok(_) => break,
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                if last_read.elapsed() > Duration::from_secs(6) {
                    shared.status.lock().error = Some("Timed out waiting for host greeting".into());
                    ctx.request_repaint();
                    return;
                }
            }
            Err(e) => {
                shared.status.lock().error = Some(e.to_string());
                ctx.request_repaint();
                return;
            }
        }
    }

    #[derive(serde::Deserialize)]
    struct Greeting {
        ok: Option<bool>,
        error: Option<String>,
        host_agent_id: Option<Uuid>,
    }

    let greeting: Greeting = match serde_json::from_str(greeting_line.trim()) {
        Ok(g) => g,
        Err(e) => {
            shared.status.lock().error = Some(format!("Invalid host response: {e}"));
            ctx.request_repaint();
            return;
        }
    };

    if !greeting.ok.unwrap_or(false) {
        let err = greeting.error.unwrap_or_else(|| "Host rejected connection".into());
        shared.status.lock().error = Some(err);
        ctx.request_repaint();
        return;
    }

    let host_agent_id = greeting.host_agent_id;
    {
        let mut st = shared.status.lock();
        st.connected = true;
        st.host_agent_id = host_agent_id;
        st.error = None;
    }
    ctx.request_repaint();

    // 4. Ingress Frame Loop
    let mut stream = reader.into_inner();
    let mut images = HashMap::new();
    let mut previous_rev = None;

    while !shared.stop.load(Ordering::Acquire) {
        let mut len_bytes = [0u8; 4];
        if let Err(e) = read_exact_timeout(&mut stream, &mut len_bytes, &shared, Duration::from_secs(10)) {
            if !shared.stop.load(Ordering::Acquire) {
                shared.status.lock().error = Some(e.to_string());
                shared.status.lock().connected = false;
                ctx.request_repaint();
            }
            break;
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        if len == 0 {
            // Heartbeat
            continue;
        }
        if len > protocol::MAX_BYTES {
            shared.status.lock().error = Some("Frame exceeds maximum size".into());
            shared.status.lock().connected = false;
            ctx.request_repaint();
            break;
        }

        let mut bytes = vec![0u8; len];
        if let Err(e) = read_exact_timeout(&mut stream, &mut bytes, &shared, Duration::from_secs(10)) {
            if !shared.stop.load(Ordering::Acquire) {
                shared.status.lock().error = Some(e.to_string());
                shared.status.lock().connected = false;
                ctx.request_repaint();
            }
            break;
        }

        if bytes.starts_with(b"SYNC") {
            if let Ok(sync) = serde_json::from_slice::<super::broadcast::InworldSync>(&bytes[4..]) {
                *shared.inworld_sync.lock() = Some(sync);
                ctx.request_repaint();
            }
            continue;
        }

        let result = protocol::prepare_staged(&bytes, &supported, pool.as_deref(), &images).and_then(|mut prepared| {
            if previous_rev.is_some_and(|old| prepared.revision <= old) {
                return Err(aurora_i18n::tr!("blender-live-invalid-revision").into());
            }
            prepared.host_agent_id = host_agent_id;
            Ok(prepared)
        });

        match result {
            Ok(prepared) => {
                let rev = prepared.revision;
                previous_rev = Some(rev);
                images = prepared.images.iter().map(|img| (img.id, img.clone())).collect();
                *shared.latest.lock() = Some(prepared);
                {
                    let mut st = shared.status.lock();
                    st.received += 1;
                    st.error = None;
                }
                ctx.request_repaint();
            }
            Err(e) => {
                shared.status.lock().error = Some(e);
                ctx.request_repaint();
            }
        }
    }

    shared.status.lock().connected = false;
    ctx.request_repaint();
}

fn read_exact_timeout(
    stream: &mut TcpStream,
    mut buf: &mut [u8],
    shared: &SpectatorShared,
    idle_timeout: Duration,
) -> io::Result<()> {
    let mut last = Instant::now();
    while !buf.is_empty() {
        if shared.stop.load(Ordering::Acquire) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        match stream.read(buf) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                last = Instant::now();
            }
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {
                if last.elapsed() >= idle_timeout {
                    return Err(io::ErrorKind::TimedOut.into());
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
