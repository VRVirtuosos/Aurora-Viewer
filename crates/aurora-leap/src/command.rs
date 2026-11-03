//! Standard LEAP Protocol Command Handler (`LLLeapListener`).
//!
//! Implements core LEAP management operations on the session's command pump:
//! `ping`, `listen`, `stoplistening`, `newpump`, `getAPIs`, `getAPI`, `getFeatures`, `getFeature`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

use aurora_llsd::{llsd_map, Llsd, Map};

use crate::codec::LeapEncoder;
use crate::pump::EventPumps;

/// Session identification for a LEAP connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeapSession {
    pub reply_pump: String,
    pub command_pump: String,
}

impl Default for LeapSession {
    fn default() -> Self {
        Self::new()
    }
}

impl LeapSession {
    /// Create a new session with freshly generated RFC 4122 v4 UUIDs.
    pub fn new() -> Self {
        Self {
            reply_pump: Uuid::new_v4().to_string(),
            command_pump: Uuid::new_v4().to_string(),
        }
    }

    /// Create a session with fixed UUIDs for deterministic testing.
    pub fn with_uuids(reply_pump: impl Into<String>, command_pump: impl Into<String>) -> Self {
        Self {
            reply_pump: reply_pump.into(),
            command_pump: command_pump.into(),
        }
    }

    /// Construct the initial handshake intro packet as an LLSD value:
    /// `{"pump": reply_pump, "data": {"command": command_pump, "features": {}}}`
    pub fn build_intro_packet(&self) -> Llsd {
        llsd_map! {
            "pump" => self.reply_pump.clone(),
            "data" => llsd_map! {
                "command" => self.command_pump.clone(),
                "features" => Llsd::new_map(),
            }
        }
    }

    /// Format the intro packet as a framed LEAP wire buffer: `<length>:<notation>`.
    pub fn build_intro_frame(&self) -> Vec<u8> {
        let packet = self.build_intro_packet();
        LeapEncoder::encode_llsd(&packet)
    }
}

/// Echo the `reqid` field from `request` into `reply` if present.
pub fn stamp_reqid(reply: &mut Llsd, request: &Llsd) {
    let reqid = request.get("reqid");
    if reqid.is_undef() {
        return;
    }

    if reply.is_undef() {
        *reply = Llsd::new_map();
    }

    if let Llsd::Map(map) = reply {
        if let Some(existing) = map.get("reqid") {
            if existing == reqid {
                return;
            }
            log::debug!(
                target: "leap::command",
                "stamp_reqid: preserving existing reqid {:?} over request reqid {:?}",
                existing, reqid
            );
            return;
        }
        map.insert("reqid".to_string(), reqid.clone());
    }
}

/// Metadata describing a single operation within an Event API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationMetadata {
    pub name: String,
    pub desc: String,
    pub required: Vec<String>,
    pub optional: Vec<String>,
}

impl OperationMetadata {
    pub fn new(name: impl Into<String>, desc: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            desc: desc.into(),
            required: Vec::new(),
            optional: Vec::new(),
        }
    }

    pub fn with_required(mut self, required: &[&str]) -> Self {
        self.required = required.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn with_optional(mut self, optional: &[&str]) -> Self {
        self.optional = optional.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn to_llsd(&self) -> Llsd {
        let mut map = llsd_map! {
            "name" => self.name.clone(),
            "desc" => self.desc.clone(),
        };
        if !self.required.is_empty() {
            let req_array: Vec<Llsd> = self
                .required
                .iter()
                .map(|s| Llsd::String(s.clone()))
                .collect();
            map.insert("required", Llsd::Array(req_array));
        }
        if !self.optional.is_empty() {
            let opt_array: Vec<Llsd> = self
                .optional
                .iter()
                .map(|s| Llsd::String(s.clone()))
                .collect();
            map.insert("optional", Llsd::Array(opt_array));
        }
        map
    }
}

/// Metadata describing an Event API for introspection via `getAPIs` and `getAPI`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiMetadata {
    pub name: String,
    pub desc: String,
    pub key: String,
    pub operations: Vec<OperationMetadata>,
}

impl ApiMetadata {
    pub fn new(name: impl Into<String>, desc: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            desc: desc.into(),
            key: "op".to_string(),
            operations: Vec::new(),
        }
    }

    pub fn add_operation(&mut self, op: OperationMetadata) {
        self.operations.push(op);
    }

    pub fn to_llsd(&self) -> Llsd {
        let ops_llsd: Vec<Llsd> = self.operations.iter().map(|op| op.to_llsd()).collect();
        llsd_map! {
            "name" => self.name.clone(),
            "desc" => self.desc.clone(),
            "key" => self.key.clone(),
            "ops" => Llsd::Array(ops_llsd),
        }
    }
}

/// Registry storing known Event APIs.
#[derive(Debug, Clone, Default)]
pub struct ApiRegistry {
    apis: HashMap<String, ApiMetadata>,
}

impl ApiRegistry {
    pub fn new() -> Self {
        Self {
            apis: HashMap::new(),
        }
    }

    pub fn register(&mut self, api: ApiMetadata) {
        self.apis.insert(api.name.clone(), api);
    }

    pub fn get(&self, name: &str) -> Option<&ApiMetadata> {
        self.apis.get(name)
    }

    pub fn list(&self) -> &HashMap<String, ApiMetadata> {
        &self.apis
    }
}

/// Errors arising from command dispatch and argument validation.
#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum CommandError {
    #[error("Protocol error: request data must be an LLSD map")]
    NotAMap,

    #[error("Dispatch error: missing required 'op' key")]
    MissingOperation,

    #[error("Dispatch error: unknown operation '{0}'")]
    UnknownOperation(String),

    #[error("Validation error: missing required argument '{0}'")]
    MissingArgument(String),

    #[error("API '{0}' not found")]
    ApiNotFound(String),

    #[error("Pump error: {0}")]
    PumpError(String),
}

/// Core protocol command handler implementing `LLLeapListener`.
pub struct LeapCommandHandler {
    pub session: LeapSession,
    api_registry: ApiRegistry,
    features: HashMap<String, String>,
    /// Tracks listeners registered via `listen` or `newpump`: (source_pump, listener_name)
    registered_listeners: HashSet<(String, String)>,
    /// Callback used to push decorated events to plugin stdout/stdin stream
    outbound_sender: Option<Arc<dyn Fn(&str, &Llsd) + Send + Sync>>,
}

impl LeapCommandHandler {
    /// Create a new command handler for a given session.
    pub fn new(session: LeapSession) -> Self {
        let mut registry = ApiRegistry::new();
        Self::register_builtin_apis(&mut registry, &session.command_pump);

        Self {
            session,
            api_registry: registry,
            features: HashMap::new(),
            registered_listeners: HashSet::new(),
            outbound_sender: None,
        }
    }

    /// Set the callback for sending outbound decorated events to the plugin.
    pub fn set_outbound_sender<F>(&mut self, sender: F)
    where
        F: Fn(&str, &Llsd) + Send + Sync + 'static,
    {
        self.outbound_sender = Some(Arc::new(sender));
    }

    /// Register default metadata for the `LLLeapListener` command pump itself.
    fn register_builtin_apis(registry: &mut ApiRegistry, command_pump: &str) {
        let mut leap_api = ApiMetadata::new(
            command_pump,
            "Operations relating to the LLSD Event API Plugin (LEAP) protocol",
        );

        leap_api.add_operation(
            OperationMetadata::new("ping", "No arguments, just a round-trip sanity check.")
                .with_optional(&["reqid", "reply"]),
        );
        leap_api.add_operation(
            OperationMetadata::new("listen", "Listen to an existing LLEventPump named ['source']")
                .with_required(&["source", "listener"])
                .with_optional(&["dest", "reqid", "reply"]),
        );
        leap_api.add_operation(
            OperationMetadata::new(
                "stoplistening",
                "Disconnect a connection established by 'listen'",
            )
            .with_required(&["source", "listener"])
            .with_optional(&["reqid", "reply"]),
        );
        leap_api.add_operation(
            OperationMetadata::new("newpump", "Instantiate a new LLEventPump and listen to it")
                .with_required(&["name"])
                .with_optional(&["type", "reqid", "reply"]),
        );
        leap_api.add_operation(
            OperationMetadata::new(
                "getAPIs",
                "Enumerate all LLEventAPI instances by name and description",
            )
            .with_optional(&["reqid", "reply"]),
        );
        leap_api.add_operation(
            OperationMetadata::new(
                "getAPI",
                "Get name, description, dispatch key and operations for LLEventAPI",
            )
            .with_required(&["api"])
            .with_optional(&["reqid", "reply"]),
        );
        leap_api.add_operation(
            OperationMetadata::new("getFeatures", "Return map of feature strings")
                .with_optional(&["reqid", "reply"]),
        );
        leap_api.add_operation(
            OperationMetadata::new("getFeature", "Return specific feature value")
                .with_required(&["feature"])
                .with_optional(&["reqid", "reply"]),
        );

        registry.register(leap_api);
    }

    /// Register a subsystem API into the introspection registry.
    pub fn register_api(&mut self, api: ApiMetadata) {
        self.api_registry.register(api);
    }

    /// Access the API registry.
    pub fn apis(&self) -> &ApiRegistry {
        &self.api_registry
    }

    /// Mutably access the API registry.
    pub fn apis_mut(&mut self) -> &mut ApiRegistry {
        &mut self.api_registry
    }

    /// Dispatch an incoming command request. Returns `Some((reply_pump, reply_data))` if a reply was requested.
    pub fn dispatch(&mut self, request: &Llsd, pumps: &EventPumps) -> Option<(String, Llsd)> {
        let reply_pump = request.get("reply").as_str().to_string();
        let expects_reply = !reply_pump.is_empty();

        let result = self.execute_op(request, pumps);

        let mut reply = match result {
            Ok(rep) => rep,
            Err(err) => {
                log::warn!(target: "leap::command", "Command error: {err}");
                llsd_map! {
                    "error" => err.to_string(),
                }
            }
        };

        if expects_reply {
            stamp_reqid(&mut reply, request);
            Some((reply_pump, reply))
        } else {
            None
        }
    }

    /// Internal operation router.
    fn execute_op(&mut self, request: &Llsd, pumps: &EventPumps) -> Result<Llsd, CommandError> {
        if !request.is_map() {
            return Err(CommandError::NotAMap);
        }

        let op = request.get("op").as_str();
        if op.is_empty() {
            return Err(CommandError::MissingOperation);
        }

        match op {
            "ping" => self.handle_ping(request),
            "listen" => self.handle_listen(request, pumps),
            "stoplistening" => self.handle_stoplistening(request, pumps),
            "newpump" => self.handle_newpump(request, pumps),
            "getAPIs" => self.handle_get_apis(request),
            "getAPI" => self.handle_get_api(request),
            "getFeatures" => self.handle_get_features(request),
            "getFeature" => self.handle_get_feature(request),
            _ => Err(CommandError::UnknownOperation(op.to_string())),
        }
    }

    // --- Core Operation Implementations ---

    fn handle_ping(&self, _request: &Llsd) -> Result<Llsd, CommandError> {
        Ok(llsd_map! {
            "status" => true,
        })
    }

    fn handle_listen(&mut self, request: &Llsd, pumps: &EventPumps) -> Result<Llsd, CommandError> {
        let source = request.get("source").as_str();
        if source.is_empty() {
            return Err(CommandError::MissingArgument("source".to_string()));
        }
        let listener = request.get("listener").as_str();
        if listener.is_empty() {
            return Err(CommandError::MissingArgument("listener".to_string()));
        }

        let key = (source.to_string(), listener.to_string());
        if self.registered_listeners.contains(&key) {
            // Duplicate listener name on same source pump rejected
            return Ok(llsd_map! {
                "status" => false,
            });
        }

        let dest = request.get("dest").as_str();
        let connected = if !dest.is_empty() {
            // Forward undecorated events from source to dest pump
            let dest_pump = dest.to_string();
            let pumps_clone = pumps.clone();
            pumps.listen(source, listener, move |ev| {
                pumps_clone.post(&dest_pump, ev);
                false
            })
        } else {
            // Forward decorated events to plugin stream
            let sender = self.outbound_sender.clone();
            let src = source.to_string();
            pumps.listen(source, listener, move |ev| {
                if let Some(ref s) = sender {
                    s(&src, ev);
                }
                false
            })
        };

        if connected {
            self.registered_listeners.insert(key);
            Ok(llsd_map! {
                "status" => true,
            })
        } else {
            Ok(llsd_map! {
                "status" => false,
            })
        }
    }

    fn handle_stoplistening(
        &mut self,
        request: &Llsd,
        pumps: &EventPumps,
    ) -> Result<Llsd, CommandError> {
        let source = request.get("source").as_str();
        if source.is_empty() {
            return Err(CommandError::MissingArgument("source".to_string()));
        }
        let listener = request.get("listener").as_str();
        if listener.is_empty() {
            return Err(CommandError::MissingArgument("listener".to_string()));
        }

        let key = (source.to_string(), listener.to_string());
        let was_registered = self.registered_listeners.remove(&key);
        let was_removed = pumps.stop_listening(source, listener);

        let status = was_registered || was_removed;
        Ok(llsd_map! {
            "status" => status,
        })
    }

    fn handle_newpump(
        &mut self,
        request: &Llsd,
        pumps: &EventPumps,
    ) -> Result<Llsd, CommandError> {
        let name = request.get("name").as_str();
        if name.is_empty() {
            return Err(CommandError::MissingArgument("name".to_string()));
        }
        let pump_type = request.get("type").as_str();
        let pump_type_str = if pump_type.is_empty() {
            "LLEventStream"
        } else {
            pump_type
        };

        let pump = pumps
            .make(name, true, pump_type_str)
            .map_err(|e| CommandError::PumpError(e.to_string()))?;

        // Automatically attach forwarder to plugin stream
        let actual_name = pump.name().to_string();
        let sender = self.outbound_sender.clone();
        let src = actual_name.clone();
        pump.listen("leap", move |ev| {
            if let Some(ref s) = sender {
                s(&src, ev);
            }
            false
        });

        Ok(llsd_map! {
            "name" => actual_name,
        })
    }

    fn handle_get_apis(&self, _request: &Llsd) -> Result<Llsd, CommandError> {
        let mut apis_map = Map::new();
        let mut root_map = Map::new();

        for (name, meta) in self.api_registry.list() {
            let desc_map = llsd_map! {
                "desc" => meta.desc.clone(),
            };
            apis_map.insert(name.clone(), desc_map.clone());
            root_map.insert(name.clone(), desc_map);
        }

        root_map.insert("apis".to_string(), Llsd::Map(apis_map));
        Ok(Llsd::Map(root_map))
    }

    fn handle_get_api(&self, request: &Llsd) -> Result<Llsd, CommandError> {
        let api_name = request.get("api").as_str();
        if api_name.is_empty() {
            return Err(CommandError::MissingArgument("api".to_string()));
        }

        match self.api_registry.get(api_name) {
            Some(meta) => Ok(meta.to_llsd()),
            None => Err(CommandError::ApiNotFound(api_name.to_string())),
        }
    }

    fn handle_get_features(&self, _request: &Llsd) -> Result<Llsd, CommandError> {
        let mut feat_map = Map::new();
        for (k, v) in &self.features {
            feat_map.insert(k.clone(), Llsd::String(v.clone()));
        }
        Ok(llsd_map! {
            "features" => Llsd::Map(feat_map),
        })
    }

    fn handle_get_feature(&self, request: &Llsd) -> Result<Llsd, CommandError> {
        let feature = request.get("feature").as_str();
        if feature.is_empty() {
            return Err(CommandError::MissingArgument("feature".to_string()));
        }

        match self.features.get(feature) {
            Some(val) => Ok(llsd_map! {
                "feature" => val.clone(),
            }),
            None => Ok(Llsd::new_map()),
        }
    }
}
