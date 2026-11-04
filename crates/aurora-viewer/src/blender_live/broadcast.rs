//! Multi-client broadcast relay for LiveMesh.
//! Allows guest spectators to connect in a non-edit (view-only) capacity.

use glam::{Quat, Vec3};
use parking_lot::{Condvar, Mutex, RwLock};
use std::{
    collections::HashMap,
    io::{self, BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use uuid::Uuid;
use super::objects::{Location, Placement};
use super::SourceState;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WireLocation {
    World,
    Attachment(u8),
    Detached,
}

impl From<Location> for WireLocation {
    fn from(loc: Location) -> Self {
        match loc {
            Location::World => WireLocation::World,
            Location::Attachment(point) => WireLocation::Attachment(point),
            Location::Detached => WireLocation::Detached,
        }
    }
}

impl From<WireLocation> for Location {
    fn from(loc: WireLocation) -> Self {
        match loc {
            WireLocation::World => Location::World,
            WireLocation::Attachment(point) => Location::Attachment(point),
            WireLocation::Detached => Location::Detached,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WireSourceState {
    Visible,
    Hidden,
    Removed,
}

impl From<SourceState> for WireSourceState {
    fn from(s: SourceState) -> Self {
        match s {
            SourceState::Visible => WireSourceState::Visible,
            SourceState::Hidden => WireSourceState::Hidden,
            SourceState::Removed => WireSourceState::Removed,
        }
    }
}

impl From<WireSourceState> for SourceState {
    fn from(s: WireSourceState) -> Self {
        match s {
            WireSourceState::Visible => SourceState::Visible,
            WireSourceState::Hidden => SourceState::Hidden,
            WireSourceState::Removed => SourceState::Removed,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WirePlacement {
    pub parent: Option<Uuid>,
    pub offset: [f32; 3],
    pub location: WireLocation,
    pub position: [f32; 3],
    pub rotation: [f32; 4],
    pub scale: [f32; 3],
    pub size: [f32; 3],
    pub center: [f32; 3],
}

impl From<Placement> for WirePlacement {
    fn from(p: Placement) -> Self {
        Self {
            parent: p.parent,
            offset: p.offset.to_array(),
            location: p.location.into(),
            position: p.position.to_array(),
            rotation: p.rotation.to_array(),
            scale: p.scale.to_array(),
            size: p.size.to_array(),
            center: p.center.to_array(),
        }
    }
}

impl WirePlacement {
    pub fn to_placement(&self) -> Placement {
        Placement {
            parent: self.parent,
            offset: Vec3::from_array(self.offset),
            location: self.location.into(),
            position: Vec3::from_array(self.position),
            rotation: Quat::from_array(self.rotation),
            scale: Vec3::from_array(self.scale),
            size: Vec3::from_array(self.size),
            center: Vec3::from_array(self.center),
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InworldSync {
    pub placements: HashMap<Uuid, WirePlacement>,
    pub states: HashMap<Uuid, WireSourceState>,
}

pub const DEFAULT_BROADCAST_PORT: u16 = 17656;
pub const GUEST_MAGIC: &[u8; 8] = b"AURGST1\n";

#[derive(Clone, Debug, Default)]
pub struct BroadcastStatus {
    pub running: bool,
    #[allow(unused)]
    pub port: u16,
    pub guests_count: usize,
    pub total_sent: u64,
    pub error: Option<String>,
}

pub struct ClientSlot {
    pub id: Uuid,
    pub pending: Mutex<Option<Arc<Vec<u8>>>>,
    pub wake: Condvar,
    pub stop: AtomicBool,
    #[allow(unused)]
    pub connected_at: Instant,
    pub frames_sent: AtomicU64,
}

pub struct Broadcaster {
    pub host_agent_id: RwLock<Option<Uuid>>,
    pub passcode: RwLock<Option<String>>,
    pub last_snapshot: Mutex<Option<(u64, Arc<Vec<u8>>)>>,
    pub last_inworld_sync: Mutex<Option<Arc<Vec<u8>>>>,
    pub clients: Mutex<Vec<Arc<ClientSlot>>>,
    pub status: RwLock<BroadcastStatus>,
    pub stop: AtomicBool,
}

pub struct Server {
    pub broadcaster: Arc<Broadcaster>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.broadcaster.stop();
    }
}

impl Broadcaster {
    pub fn new(port: u16, passcode: Option<String>, host_agent_id: Option<Uuid>) -> Self {
        Self {
            host_agent_id: RwLock::new(host_agent_id),
            passcode: RwLock::new(passcode),
            last_snapshot: Mutex::new(None),
            last_inworld_sync: Mutex::new(None),
            clients: Mutex::new(Vec::new()),
            status: RwLock::new(BroadcastStatus {
                running: true,
                port,
                guests_count: 0,
                total_sent: 0,
                error: None,
            }),
            stop: AtomicBool::new(false),
        }
    }

    pub fn set_host_agent_id(&self, id: Option<Uuid>) {
        *self.host_agent_id.write() = id;
    }

    #[allow(unused)]
    pub fn set_passcode(&self, passcode: Option<String>) {
        *self.passcode.write() = passcode;
    }

    pub fn broadcast(&self, revision: u64, payload: &[u8]) {
        let arc_payload = Arc::new(payload.to_vec());
        *self.last_snapshot.lock() = Some((revision, arc_payload.clone()));

        let clients = self.clients.lock();
        for client in clients.iter() {
            *client.pending.lock() = Some(arc_payload.clone());
            client.wake.notify_one();
        }
        self.status.write().total_sent += 1;
    }

    pub fn broadcast_inworld_sync(&self, sync: &InworldSync) {
        if let Ok(json_bytes) = serde_json::to_vec(sync) {
            let mut packet = b"SYNC".to_vec();
            packet.extend(json_bytes);
            let arc_packet = Arc::new(packet);
            *self.last_inworld_sync.lock() = Some(arc_packet.clone());

            let clients = self.clients.lock();
            for client in clients.iter() {
                *client.pending.lock() = Some(arc_packet.clone());
                client.wake.notify_one();
            }
        }
    }

    pub fn stop(&self) {
        if self.stop.swap(true, Ordering::AcqRel) {
            return;
        }
        self.status.write().running = false;
        let clients = self.clients.lock();
        for client in clients.iter() {
            client.stop.store(true, Ordering::Release);
            client.wake.notify_one();
        }
    }
}

pub fn start(
    port: u16,
    passcode: Option<String>,
    host_agent_id: Option<Uuid>,
    ctx: egui::Context,
) -> io::Result<Server> {
    let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port))?;
    listener.set_nonblocking(true)?;

    let broadcaster = Arc::new(Broadcaster::new(port, passcode, host_agent_id));
    let worker_broadcaster = broadcaster.clone();

    std::thread::Builder::new()
        .name("blender-live-broadcast".into())
        .spawn(move || {
            while !worker_broadcaster.stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _peer)) => {
                        let b = worker_broadcaster.clone();
                        let c = ctx.clone();
                        std::thread::Builder::new()
                            .name("blender-guest-worker".into())
                            .spawn(move || {
                                handle_guest_connection(stream, b, c);
                            })
                            .ok();
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(40));
                    }
                    Err(e) => {
                        if !worker_broadcaster.stop.load(Ordering::Acquire) {
                            worker_broadcaster.status.write().error = Some(e.to_string());
                            ctx.request_repaint();
                        }
                        break;
                    }
                }
            }
        })?;

    Ok(Server { broadcaster })
}

fn handle_guest_connection(mut stream: TcpStream, broadcaster: Arc<Broadcaster>, ctx: egui::Context) {
    if let Err(_) = stream.set_read_timeout(Some(Duration::from_secs(4))) {
        return;
    }
    if let Err(_) = stream.set_write_timeout(Some(Duration::from_secs(5))) {
        return;
    }
    if let Err(_) = stream.set_nodelay(true) {
        return;
    }

    // 1. Verify Magic
    let mut magic = [0u8; 8];
    if stream.read_exact(&mut magic).is_err() || &magic != GUEST_MAGIC {
        return;
    }

    // 2. Read Auth JSON Header
    let read_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut reader = BufReader::new(read_stream);
    let mut auth_line = String::new();
    if reader.read_line(&mut auth_line).is_err() {
        return;
    }

    #[derive(serde::Deserialize)]
    struct AuthHeader {
        passcode: Option<String>,
        #[allow(unused)]
        client_agent_id: Option<Uuid>,
    }

    let auth: AuthHeader = serde_json::from_str(auth_line.trim()).unwrap_or(AuthHeader {
        passcode: None,
        client_agent_id: None,
    });

    let expected = broadcaster.passcode.read().clone();
    if let Some(expected_passcode) = expected {
        if !expected_passcode.is_empty() && auth.passcode.as_deref() != Some(&expected_passcode) {
            let _ = writeln!(
                stream,
                "{}",
                serde_json::json!({
                    "ok": false,
                    "error": "Invalid passcode"
                })
            );
            return;
        }
    }

    // 3. Send Guest Greeting
    let host_id = *broadcaster.host_agent_id.read();
    let greeting = serde_json::json!({
        "ok": true,
        "protocol": super::protocol::VERSION,
        "role": "spectator",
        "host_agent_id": host_id,
        "max_bytes": super::protocol::MAX_BYTES,
        "features": [
            "pbr-materials",
            "source-ids",
            "static-meshes",
            "object-links",
            "compressed-snapshots",
            "packed-geometry"
        ]
    });

    if writeln!(stream, "{greeting}").is_err() {
        return;
    }

    // 4. Register client slot
    let slot = Arc::new(ClientSlot {
        id: Uuid::new_v4(),
        pending: Mutex::new(None),
        wake: Condvar::new(),
        stop: AtomicBool::new(false),
        connected_at: Instant::now(),
        frames_sent: AtomicU64::new(0),
    });

    // If snapshot or inworld sync already exist, immediately seed this guest
    if let Some((_rev, ref frame)) = *broadcaster.last_snapshot.lock() {
        let len = (frame.len() as u32).to_le_bytes();
        let _ = stream.write_all(&len);
        let _ = stream.write_all(frame);
    }
    if let Some(ref sync_frame) = *broadcaster.last_inworld_sync.lock() {
        let len = (sync_frame.len() as u32).to_le_bytes();
        let _ = stream.write_all(&len);
        let _ = stream.write_all(sync_frame);
    }
    let _ = stream.flush();

    {
        let mut clients = broadcaster.clients.lock();
        clients.push(slot.clone());
        broadcaster.status.write().guests_count = clients.len();
        ctx.request_repaint();
    }

    // 5. Client transmission loop (with conflation & heartbeats)
    let mut last_send = Instant::now();
    while !broadcaster.stop.load(Ordering::Acquire) && !slot.stop.load(Ordering::Acquire) {
        let mut lock = slot.pending.lock();
        if lock.is_none() {
            let timeout = Duration::from_millis(200).saturating_sub(last_send.elapsed());
            if timeout.is_zero() {
                drop(lock);
                // Zero-length heartbeat to keep connection alive
                if stream.write_all(&0u32.to_le_bytes()).is_err() || stream.flush().is_err() {
                    break;
                }
                last_send = Instant::now();
                continue;
            }
            slot.wake.wait_for(&mut lock, timeout);
        }
        let frame = lock.take();
        drop(lock);

        if let Some(frame) = frame {
            let len = (frame.len() as u32).to_le_bytes();
            if stream.write_all(&len).is_err()
                || stream.write_all(&frame).is_err()
                || stream.flush().is_err()
            {
                break;
            }
            slot.frames_sent.fetch_add(1, Ordering::Relaxed);
            last_send = Instant::now();
        }
    }

    // 6. Cleanup on disconnect
    slot.stop.store(true, Ordering::Release);
    {
        let mut clients = broadcaster.clients.lock();
        clients.retain(|c| c.id != slot.id);
        broadcaster.status.write().guests_count = clients.len();
        ctx.request_repaint();
    }
}
