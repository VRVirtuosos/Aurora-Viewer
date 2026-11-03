//! Thread-safe event pump registry and pub/sub message dispatching.
//!
//! Implements `LLEventStream`, `LLEventMailDrop`, and `EventPumps` registry
//! matching Second Life viewer semantics.

use aurora_llsd::Llsd;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Callback function invoked when an event is posted to an EventPump.
/// Returning `true` indicates the event was handled and stops propagation to subsequent listeners.
pub type ListenerCallback = Arc<dyn Fn(&Llsd) -> bool + Send + Sync>;

/// Errors returned by the EventPump system.
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum PumpError {
    #[error("duplicate pump name: '{0}'")]
    DupPumpName(String),

    #[error("unsupported pump type: '{0}'")]
    BadType(String),

    #[error("pump not found: '{0}'")]
    NotFound(String),
}

/// Type of event pump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpType {
    Stream,
    MailDrop,
}

#[derive(Clone)]
struct NamedListener {
    name: String,
    callback: ListenerCallback,
}

enum PumpStorage {
    Stream,
    MailDrop {
        history: Mutex<VecDeque<Llsd>>,
    },
}

/// An individual named pub/sub channel.
pub struct EventPump {
    name: String,
    pump_type: PumpType,
    enabled: AtomicBool,
    listeners: RwLock<Vec<NamedListener>>,
    storage: PumpStorage,
}

impl std::fmt::Debug for EventPump {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventPump")
            .field("name", &self.name)
            .field("pump_type", &self.pump_type)
            .field("enabled", &self.enabled())
            .finish()
    }
}

impl EventPump {
    /// Create a new LLEventStream.
    pub fn new_stream(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pump_type: PumpType::Stream,
            enabled: AtomicBool::new(true),
            listeners: RwLock::new(Vec::new()),
            storage: PumpStorage::Stream,
        }
    }

    /// Create a new LLEventMailDrop.
    pub fn new_maildrop(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pump_type: PumpType::MailDrop,
            enabled: AtomicBool::new(true),
            listeners: RwLock::new(Vec::new()),
            storage: PumpStorage::MailDrop {
                history: Mutex::new(VecDeque::new()),
            },
        }
    }

    /// The name of this pump.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The pump type (Stream or MailDrop).
    pub fn pump_type(&self) -> PumpType {
        self.pump_type
    }

    /// Check if pump is enabled.
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Enable or disable this pump.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    /// Subscribe a listener callback by unique name.
    /// Returns `true` if connected, or `false` if a listener with this name is already registered.
    pub fn listen<F>(&self, listener_name: &str, callback: F) -> bool
    where
        F: Fn(&Llsd) -> bool + Send + Sync + 'static,
    {
        self.listen_arc(listener_name, Arc::new(callback))
    }

    /// Subscribe an Arc-wrapped listener callback.
    pub fn listen_arc(&self, listener_name: &str, callback: ListenerCallback) -> bool {
        {
            let mut listeners = self.listeners.write();
            if listeners.iter().any(|l| l.name == listener_name) {
                return false; // Reject duplicate listener
            }
            listeners.push(NamedListener {
                name: listener_name.to_string(),
                callback: callback.clone(),
            });
        }

        // If this is a MailDrop, flush pending history to the newly attached listener
        if let PumpStorage::MailDrop { ref history } = self.storage {
            let mut hist = history.lock();
            let mut i = 0;
            while i < hist.len() {
                if callback(&hist[i]) {
                    // Handled: consume and remove from history
                    hist.remove(i);
                } else {
                    i += 1;
                }
            }
        }

        true
    }

    /// Disconnect a listener by name.
    /// Returns `true` if removed, `false` if not found.
    pub fn stop_listening(&self, listener_name: &str) -> bool {
        let mut listeners = self.listeners.write();
        if let Some(pos) = listeners.iter().position(|l| l.name == listener_name) {
            listeners.remove(pos);
            true
        } else {
            false
        }
    }

    /// Test if a listener is registered.
    pub fn has_listener(&self, listener_name: &str) -> bool {
        let listeners = self.listeners.read();
        listeners.iter().any(|l| l.name == listener_name)
    }

    /// List all registered listener names in registration order.
    pub fn listener_names(&self) -> Vec<String> {
        let listeners = self.listeners.read();
        listeners.iter().map(|l| l.name.clone()).collect()
    }

    /// Clear all listeners.
    pub fn clear(&self) {
        let mut listeners = self.listeners.write();
        listeners.clear();
    }

    /// Discard stored history (for MailDrop).
    pub fn discard(&self) {
        if let PumpStorage::MailDrop { ref history } = self.storage {
            history.lock().clear();
        }
    }

    /// Number of queued messages in history (for MailDrop).
    pub fn history_len(&self) -> usize {
        if let PumpStorage::MailDrop { ref history } = self.storage {
            history.lock().len()
        } else {
            0
        }
    }

    /// Post an event to all registered listeners.
    /// Deadlock-free: clones listener callbacks and releases lock before executing them.
    pub fn post(&self, event: &Llsd) -> bool {
        if !self.enabled() {
            return false;
        }

        // Snapshot listeners to execute outside lock
        let snapshot = {
            let guard = self.listeners.read();
            guard.clone()
        };

        let mut handled = false;
        for listener in snapshot {
            if (listener.callback)(event) {
                handled = true;
                break;
            }
        }

        // If MailDrop and unhandled, queue into history
        if !handled {
            if let PumpStorage::MailDrop { ref history } = self.storage {
                history.lock().push_back(event.clone());
            }
        }

        handled
    }
}

/// Global/central registry of named EventPumps.
#[derive(Clone)]
pub struct EventPumps {
    pumps: Arc<RwLock<HashMap<String, Arc<EventPump>>>>,
}

impl Default for EventPumps {
    fn default() -> Self {
        Self::new()
    }
}

impl EventPumps {
    /// Create a new EventPumps registry.
    pub fn new() -> Self {
        Self {
            pumps: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Obtain an existing pump, or create an LLEventStream if not found.
    pub fn obtain(&self, name: &str) -> Arc<EventPump> {
        {
            let read_guard = self.pumps.read();
            if let Some(pump) = read_guard.get(name) {
                return pump.clone();
            }
        }

        let mut write_guard = self.pumps.write();
        // Double-check under write lock
        if let Some(pump) = write_guard.get(name) {
            return pump.clone();
        }

        let new_pump = Arc::new(EventPump::new_stream(name));
        write_guard.insert(name.to_string(), new_pump.clone());
        new_pump
    }

    /// Find an existing pump without auto-instantiating.
    pub fn find(&self, name: &str) -> Option<Arc<EventPump>> {
        let read_guard = self.pumps.read();
        read_guard.get(name).cloned()
    }

    /// Instantiate a new pump with explicit type and collision handling.
    pub fn make(
        &self,
        name: &str,
        tweak: bool,
        pump_type: &str,
    ) -> Result<Arc<EventPump>, PumpError> {
        let mut write_guard = self.pumps.write();

        let final_name = if tweak {
            self.tweak_name_locked(&write_guard, name)
        } else {
            if write_guard.contains_key(name) {
                return Err(PumpError::DupPumpName(name.to_string()));
            }
            name.to_string()
        };

        let pump = match pump_type {
            "LLEventStream" | "" => Arc::new(EventPump::new_stream(&final_name)),
            "LLEventMailDrop" => Arc::new(EventPump::new_maildrop(&final_name)),
            other => return Err(PumpError::BadType(other.to_string())),
        };

        write_guard.insert(final_name, pump.clone());
        Ok(pump)
    }

    fn tweak_name_locked(&self, map: &HashMap<String, Arc<EventPump>>, base_name: &str) -> String {
        if !map.contains_key(base_name) {
            return base_name.to_string();
        }
        let mut counter = 1;
        loop {
            let candidate = format!("{}_{}", base_name, counter);
            if !map.contains_key(&candidate) {
                return candidate;
            }
            counter += 1;
        }
    }

    /// Post an event to a named pump if it exists. Returns false if not found.
    pub fn post(&self, name: &str, event: &Llsd) -> bool {
        if let Some(pump) = self.find(name) {
            pump.post(event);
            true
        } else {
            false
        }
    }

    /// Subscribe a listener to a named pump (creating it as a Stream if absent).
    pub fn listen<F>(&self, pump_name: &str, listener_name: &str, callback: F) -> bool
    where
        F: Fn(&Llsd) -> bool + Send + Sync + 'static,
    {
        let pump = self.obtain(pump_name);
        pump.listen(listener_name, callback)
    }

    /// Disconnect a listener from a named pump.
    pub fn stop_listening(&self, pump_name: &str, listener_name: &str) -> bool {
        if let Some(pump) = self.find(pump_name) {
            pump.stop_listening(listener_name)
        } else {
            false
        }
    }

    /// List all registered pump names.
    pub fn pump_names(&self) -> Vec<String> {
        let read_guard = self.pumps.read();
        read_guard.keys().cloned().collect()
    }

    /// Disconnect all listeners on all pumps.
    pub fn clear(&self) {
        let read_guard = self.pumps.read();
        for pump in read_guard.values() {
            pump.clear();
        }
    }
}
