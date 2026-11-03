//! Modular LEAP Subsystem Dispatch and Event Broadcaster.
//!
//! Connects native `aurora-leap` event pumps to internal Aurora-Viewer
//! subsystems (`LLAppViewer`, `LLViewerControl`, `LLChat`, `LLAgent`, `LLLogin`, `LLCamera`, `LLInventory`).

use std::time::Instant;
use uuid::Uuid;

use aurora_leap::{
    stamp_reqid, ApiMetadata, LeapManager, LeapMessage, OperationMetadata,
};
use aurora_llsd::{llsd_map, Llsd, Map};
use aurora_net::{ChatType, NetCommand, NetEvent};

use crate::app::{App, Screen};

/// Initialize LeapManager from command-line arguments.
pub fn init_leap() -> Option<LeapManager> {
    let cli = crate::cli::get();
    let mut manager = if let Some(cmd) = cli.leap.as_deref() {
        log::info!(target: "leap", "Starting LEAP subprocess transport: '{cmd}'");
        match LeapManager::spawn_subprocess(cmd) {
            Ok(mgr) => mgr,
            Err(err) => {
                log::error!(target: "leap", "Failed to launch LEAP subprocess '{cmd}': {err}");
                return None;
            }
        }
    } else if cli.leap_stdio {
        log::info!(target: "leap", "Attaching LEAP direct stdio transport");
        match LeapManager::attach_stdio() {
            Ok(mgr) => mgr,
            Err(err) => {
                log::error!(target: "leap", "Failed to attach LEAP stdio: {err}");
                return None;
            }
        }
    } else {
        return None;
    };

    register_default_pumps(&mut manager);
    Some(manager)
}

/// Register default subsystem pumps and APIs into LeapManager.
pub fn register_default_pumps(manager: &mut LeapManager) {
    for pump_name in &[
        "LLAppViewer",
        "LLViewerControl",
        "LLChat",
        "LLAgent",
        "LLLogin",
        "LLCamera",
        "LLInventory",
        "LLWorld",
        "LLSimulator",
        "LLNearbyAvatars",
        "LLTeleport",
        "LLStartUp",
        "StartupState",
        "teleport",
        "chat",
        "im",
        "LLInstantMessage",
        "login",
        "settings",
        "world",
        "radar",
        "LLObjects",
        "objects",
        "LLSelect",
        "select",
        "LLAppearance",
        "appearance",
        "LLAnimation",
        "animation",
        "animations",
        "LLVoice",
        "voice",
        "LLLeapListener",
        "leap_listener",
    ] {
        manager.pumps().obtain(pump_name);
    }

    register_subsystem_apis(manager);
    broadcast_startup_state(manager, "STATE_FIRST", "Viewer initialized");
}

/// Register standard subsystem APIs for introspection via `getAPIs` and `getAPI`.
pub fn register_subsystem_apis(manager: &mut LeapManager) {
    // 1. LLAppViewer
    let mut app_api = ApiMetadata::new("LLAppViewer", "Viewer application lifecycle and runtime diagnostics");
    app_api.add_operation(
        OperationMetadata::new("ping", "Round-trip sanity check")
            .with_optional(&["reqid", "reply"]),
    );
    app_api.add_operation(
        OperationMetadata::new("requestQuit", "Request graceful application shutdown")
            .with_optional(&["reqid", "reply"]),
    );
    app_api.add_operation(
        OperationMetadata::new("forceQuit", "Force immediate process termination")
            .with_optional(&["reqid", "reply"]),
    );
    app_api.add_operation(
        OperationMetadata::new("status", "Query runtime metrics (FPS, uptime, version, frame count)")
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(app_api);

    // 2. LLViewerControl
    let mut ctrl_api = ApiMetadata::new("LLViewerControl", "Viewer configuration settings access and mutation");
    ctrl_api.add_operation(
        OperationMetadata::new("get", "Query setting variable value")
            .with_required(&["key"])
            .with_optional(&["name", "reqid", "reply"]),
    );
    ctrl_api.add_operation(
        OperationMetadata::new("set", "Mutate setting variable value")
            .with_required(&["key", "value"])
            .with_optional(&["reqid", "reply"]),
    );
    ctrl_api.add_operation(
        OperationMetadata::new("toggle", "Toggle boolean setting variable")
            .with_required(&["key"])
            .with_optional(&["reqid", "reply"]),
    );
    ctrl_api.add_operation(
        OperationMetadata::new("groups", "List setting categories")
            .with_optional(&["reqid", "reply"]),
    );
    ctrl_api.add_operation(
        OperationMetadata::new("vars", "Enumerate all setting variables")
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(ctrl_api);

    // 3. LLChat
    let mut chat_api = ApiMetadata::new("LLChat", "Chat transmission and interaction");
    chat_api.add_operation(
        OperationMetadata::new("sendChat", "Send chat message into simulator")
            .with_required(&["message"])
            .with_optional(&["channel", "type", "text", "reqid", "reply"]),
    );
    manager.register_api(chat_api);

    // 4. LLAgent
    let mut agent_api = ApiMetadata::new("LLAgent", "Avatar identity and spatial telemetry");
    agent_api.add_operation(
        OperationMetadata::new("getPosition", "Get current avatar position in region coordinates")
            .with_optional(&["reqid", "reply"]),
    );
    agent_api.add_operation(
        OperationMetadata::new("getID", "Get current avatar agent UUID")
            .with_optional(&["reqid", "reply"]),
    );
    agent_api.add_operation(
        OperationMetadata::new("move", "Move avatar in direction or step offsets")
            .with_optional(&["direction", "step", "speed", "dx", "dy", "dz", "reqid", "reply"]),
    );
    agent_api.add_operation(
        OperationMetadata::new("teleport", "Teleport avatar to region coordinates")
            .with_optional(&["x", "y", "z", "region", "reqid", "reply"]),
    );
    manager.register_api(agent_api);

    // 5. LLLogin
    let mut login_api = ApiMetadata::new("LLLogin", "Authentication and session status");
    login_api.add_operation(
        OperationMetadata::new("getState", "Query startup and authentication state")
            .with_optional(&["reqid", "reply"]),
    );
    login_api.add_operation(
        OperationMetadata::new("login", "Authenticate avatar on grid")
            .with_optional(&["first", "last", "username", "password", "uri", "start", "reqid", "reply"]),
    );
    login_api.add_operation(
        OperationMetadata::new("logout", "Request session logout from simulator")
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(login_api);

    // 6. LLCamera
    let mut cam_api = ApiMetadata::new("LLCamera", "Camera manipulation and inspection");
    cam_api.add_operation(
        OperationMetadata::new("get", "Get camera position, focus target and FOV")
            .with_optional(&["reqid", "reply"]),
    );
    cam_api.add_operation(
        OperationMetadata::new("set", "Set camera position, focus target, and/or FOV")
            .with_optional(&["position", "x", "y", "z", "target", "target_x", "target_y", "target_z", "fov", "reqid", "reply"]),
    );
    cam_api.add_operation(
        OperationMetadata::new("look_at", "Aim camera focus target at world coordinates")
            .with_optional(&["target", "x", "y", "z", "reqid", "reply"]),
    );
    cam_api.add_operation(
        OperationMetadata::new("look_at_avatar", "Aim camera focus target directly at avatar head")
            .with_optional(&["reqid", "reply"]),
    );
    cam_api.add_operation(
        OperationMetadata::new("orbit", "Orbit camera position around current focus target by yaw and pitch degrees")
            .with_optional(&["yaw_deg", "pitch_deg", "yaw", "pitch", "reqid", "reply"]),
    );
    cam_api.add_operation(
        OperationMetadata::new("pan", "Translate camera position and target in camera-relative coordinate space")
            .with_optional(&["forward", "strafe", "elevation", "reqid", "reply"]),
    );
    cam_api.add_operation(
        OperationMetadata::new("preset", "Apply standard framing preset (portrait, full_body, wide_landscape, overhead)")
            .with_optional(&["preset", "name", "reqid", "reply"]),
    );
    cam_api.add_operation(
        OperationMetadata::new("reset", "Reset camera view to default avatar orientation")
            .with_optional(&["reqid", "reply"]),
    );
    cam_api.add_operation(
        OperationMetadata::new("takePicture", "Capture an inworld snapshot photo and return PNG image bytes")
            .with_optional(&["width", "height", "save_to", "format", "reqid", "reply"]),
    );
    manager.register_api(cam_api);

    // 7. LLInventory
    let mut inv_api = ApiMetadata::new("LLInventory", "Inventory hierarchy inspection");
    inv_api.add_operation(
        OperationMetadata::new("getRoot", "Query inventory root folder UUID")
            .with_optional(&["reqid", "reply"]),
    );
    inv_api.add_operation(
        OperationMetadata::new("getItem", "Query item metadata by UUID")
            .with_required(&["item_id"])
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(inv_api);

    // 8. LLWorld / LLSimulator
    let mut world_api = ApiMetadata::new("LLWorld", "Simulator region and parcel spatial telemetry");
    world_api.add_operation(
        OperationMetadata::new("getRegionInfo", "Query active simulator region details and dimensions")
            .with_optional(&["reqid", "reply"]),
    );
    world_api.add_operation(
        OperationMetadata::new("getParcelInfo", "Query parcel details, area, ownership, and flags")
            .with_optional(&["reqid", "reply"]),
    );
    world_api.add_operation(
        OperationMetadata::new("getSimStats", "Query simulator performance statistics, avatar count, and uptime")
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(world_api);

    // 9. LLNearbyAvatars
    let mut radar_api = ApiMetadata::new("LLNearbyAvatars", "Radar tracking of nearby residents and avatars");
    radar_api.add_operation(
        OperationMetadata::new("getNearby", "Enumerate all avatars in draw distance with coordinates and distances")
            .with_optional(&["max_distance", "reqid", "reply"]),
    );
    radar_api.add_operation(
        OperationMetadata::new("getAvatarDetails", "Query detailed coordinates and status for an avatar by UUID")
            .with_required(&["avatar_id"])
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(radar_api);

    // 10. LLObjects / LLSelect
    let mut objects_api = ApiMetadata::new("LLObjects", "Spatial querying, selection, and inspection of simulator objects and prims");
    objects_api.add_operation(
        OperationMetadata::new("getNearbyObjects", "Query nearby objects and primitives in draw distance")
            .with_optional(&["max_distance", "limit", "include_attachments", "reqid", "reply"]),
    );
    objects_api.add_operation(
        OperationMetadata::new("inspect", "Inspect detailed parameters and geometry for a specific object by local_id or uuid")
            .with_optional(&["local_id", "uuid", "reqid", "reply"]),
    );
    objects_api.add_operation(
        OperationMetadata::new("selectObject", "Select an object inworld by local_id or uuid")
            .with_optional(&["local_id", "uuid", "reqid", "reply"]),
    );
    objects_api.add_operation(
        OperationMetadata::new("deselect", "Deselect active object selection")
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(objects_api);

    // 11. LLAppearance
    let mut app_api = ApiMetadata::new("LLAppearance", "Avatar appearance, worn outfit, and attachment inspection");
    app_api.add_operation(
        OperationMetadata::new("getWornItems", "Enumerate worn clothing items, attachments, and Current Outfit Folder (COF)")
            .with_optional(&["reqid", "reply"]),
    );
    app_api.add_operation(
        OperationMetadata::new("getBakeStatus", "Query avatar texture bake and mesh pipeline status")
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(app_api);

    // 12. LLAnimation
    let mut anim_api = ApiMetadata::new("LLAnimation", "Playback and telemetry of avatar animations");
    anim_api.add_operation(
        OperationMetadata::new("getActiveAnimations", "Query currently active and playing animation UUIDs on avatar")
            .with_optional(&["avatar_id", "reqid", "reply"]),
    );
    anim_api.add_operation(
        OperationMetadata::new("startAnimation", "Trigger playback of an animation by UUID")
            .with_required(&["anim_id"])
            .with_optional(&["reqid", "reply"]),
    );
    anim_api.add_operation(
        OperationMetadata::new("stopAnimation", "Stop animation playback")
            .with_optional(&["anim_id", "reqid", "reply"]),
    );
    manager.register_api(anim_api);

    // 13. LLVoice
    let mut voice_api = ApiMetadata::new("LLVoice", "Spatial voice and WebRTC telemetry and control");
    voice_api.add_operation(
        OperationMetadata::new("getStatus", "Query voice enabled status, mic gain, and talking indicator")
            .with_optional(&["reqid", "reply"]),
    );
    voice_api.add_operation(
        OperationMetadata::new("setMuted", "Set voice microphone mute state")
            .with_required(&["muted"])
            .with_optional(&["reqid", "reply"]),
    );
    voice_api.add_operation(
        OperationMetadata::new("setVolume", "Set microphone gain or audio output volume")
            .with_required(&["volume"])
            .with_optional(&["reqid", "reply"]),
    );
    voice_api.add_operation(
        OperationMetadata::new("joinChannel", "Join a spatial or group voice channel URI")
            .with_required(&["uri"])
            .with_optional(&["channel", "reqid", "reply"]),
    );
    voice_api.add_operation(
        OperationMetadata::new("leaveChannel", "Leave the current active voice channel")
            .with_optional(&["reqid", "reply"]),
    );
    manager.register_api(voice_api);

    // 14. LLLeapListener
    let mut listener_api = ApiMetadata::new("LLLeapListener", "Dynamic pump subscription and listener management");
    listener_api.add_operation(
        OperationMetadata::new("listen", "Register a listener callback forwarding events from a source pump")
            .with_required(&["source", "listener"])
            .with_optional(&["reqid", "reply"]),
    );
    listener_api.add_operation(
        OperationMetadata::new("stopListening", "Unregister a listener from a source pump")
            .with_required(&["source", "listener"])
            .with_optional(&["reqid", "reply"]),
    );
    listener_api.add_operation(
        OperationMetadata::new("getListeners", "Query active listeners across all or specific pumps")
            .with_optional(&["source", "reqid", "reply"]),
    );
    manager.register_api(listener_api);
}

/// Dispatch an incoming LEAP message destined for a viewer subsystem.
pub fn dispatch_subsystem_message(app: &mut App, msg: LeapMessage) {
    let reply_pump = msg.data.get("reply").as_str().to_string();
    let op = msg.data.get("op").as_str();

    match msg.pump.as_str() {
        "LLAppViewer" | "app" => {
            handle_app_viewer(app, op, &msg.data, &reply_pump);
        }
        "LLViewerControl" | "viewer_control" | "settings" => {
            handle_viewer_control(app, op, &msg.data, &reply_pump);
        }
        "LLChat" | "chat" => {
            handle_chat(app, op, &msg.data, &reply_pump);
        }
        "LLAgent" | "agent" => {
            handle_agent(app, op, &msg.data, &reply_pump);
        }
        "LLLogin" | "login" | "LLStartUp" | "startup" => {
            handle_login(app, op, &msg.data, &reply_pump);
        }
        "LLCamera" | "camera" => {
            handle_camera(app, op, &msg.data, &reply_pump);
        }
        "LLInventory" | "inventory" => {
            handle_inventory(app, op, &msg.data, &reply_pump);
        }
        "LLWorld" | "world" | "LLSimulator" | "simulator" => {
            handle_world(app, op, &msg.data, &reply_pump);
        }
        "LLNearbyAvatars" | "nearby_avatars" | "radar" => {
            handle_nearby_avatars(app, op, &msg.data, &reply_pump);
        }
        "LLObjects" | "objects" | "LLSelect" | "select" => {
            handle_objects(app, op, &msg.data, &reply_pump);
        }
        "LLAppearance" | "appearance" => {
            handle_appearance(app, op, &msg.data, &reply_pump);
        }
        "LLAnimation" | "animation" | "animations" => {
            handle_animation(app, op, &msg.data, &reply_pump);
        }
        "LLVoice" | "voice" => {
            handle_voice(app, op, &msg.data, &reply_pump);
        }
        "LLLeapListener" | "leap_listener" => {
            handle_leap_listener(app, op, &msg.data, &reply_pump);
        }
        other => {
            log::warn!(target: "leap", "dispatch: unhandled subsystem pump '{other}'");
            if !reply_pump.is_empty() {
                let err_reply = llsd_map! {
                    "error" => format!("Unhandled subsystem pump '{other}'"),
                };
                send_reply(app, &reply_pump, err_reply, &msg.data);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Subsystem Handlers
// ---------------------------------------------------------------------------

fn handle_app_viewer(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "ping" => {
            let reply = llsd_map! { "status" => true };
            send_reply(app, reply_pump, reply, data);
        }
        "requestQuit" | "quit" => {
            log::info!(target: "leap", "LLAppViewer: requestQuit triggered");
            app.quit_at = Some(Instant::now());
            let reply = llsd_map! { "status" => true };
            send_reply(app, reply_pump, reply, data);
        }
        "forceQuit" => {
            log::info!(target: "leap", "LLAppViewer: forceQuit triggered");
            let reply = llsd_map! { "status" => true };
            send_reply(app, reply_pump, reply, data);
            std::process::exit(0);
        }
        "status" => {
            let fps = if app.prev_dt > 0.0 { (1.0 / app.prev_dt) as f64 } else { 0.0 };
            let uptime = app.start.elapsed().as_secs_f64();
            let version = aurora_version::version();
            let screen_str = match app.screen {
                Screen::Login => "Login",
                Screen::Loading { .. } => "Loading",
                Screen::World => "World",
            };
            let reply = llsd_map! {
                "fps" => fps,
                "uptime" => uptime,
                "version" => version,
                "frame_count" => app.frame_count as i32,
                "screen" => screen_str,
                "logged_in" => app.world.login.is_some(),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "getAPIs" | "get_apis" => {
            if let Some(leap) = &app.leap {
                let mut apis_map = Map::new();
                for (name, meta) in leap.apis().list() {
                    apis_map.insert(name.clone(), llsd_map! { "desc" => meta.desc.clone() });
                }
                let reply = llsd_map! {
                    "status" => true,
                    "apis" => Llsd::Map(apis_map),
                };
                send_reply(app, reply_pump, reply, data);
            }
        }
        "getAPI" | "get_api" => {
            let api_name = data.get("api").as_str();
            if let Some(leap) = &app.leap {
                if let Some(meta) = leap.apis().get(api_name) {
                    send_reply(app, reply_pump, meta.to_llsd(), data);
                } else {
                    let reply = llsd_map! { "status" => false, "error" => format!("API '{api_name}' not found") };
                    send_reply(app, reply_pump, reply, data);
                }
            }
        }
        "getListeners" | "get_listeners" => {
            if let Some(leap) = &app.leap {
                let mut all = Vec::new();
                for name in leap.pumps().pump_names() {
                    if let Some(p) = leap.pumps().find(&name) {
                        for l in p.listener_names() {
                            all.push(llsd_map! { "pump" => name.clone(), "listener" => l });
                        }
                    }
                }
                let reply = llsd_map! { "status" => true, "listeners" => Llsd::Array(all) };
                send_reply(app, reply_pump, reply, data);
            }
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLAppViewer"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_viewer_control(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "get" => {
            let key = if data.has("key") {
                data.get("key").as_str()
            } else {
                data.get("name").as_str()
            };
            if key.is_empty() {
                let reply = llsd_map! { "error" => "Missing required 'key' argument" };
                send_reply(app, reply_pump, reply, data);
                return;
            }

            match query_setting(&app.settings, key) {
                Some(val) => {
                    let reply = llsd_map! {
                        "key" => key,
                        "value" => val,
                    };
                    send_reply(app, reply_pump, reply, data);
                }
                None => {
                    let reply = llsd_map! {
                        "error" => format!("Unknown setting variable '{key}'"),
                    };
                    send_reply(app, reply_pump, reply, data);
                }
            }
        }
        "set" => {
            let key = data.get("key").as_str();
            let val = data.get("value");
            if key.is_empty() {
                let reply = llsd_map! { "error" => "Missing required 'key' argument" };
                send_reply(app, reply_pump, reply, data);
                return;
            }
            if val.is_undef() {
                let reply = llsd_map! { "error" => "Missing required 'value' argument" };
                send_reply(app, reply_pump, reply, data);
                return;
            }

            match mutate_setting(app, key, val) {
                Ok(new_val) => {
                    let reply = llsd_map! {
                        "key" => key,
                        "status" => true,
                        "value" => new_val,
                    };
                    send_reply(app, reply_pump, reply, data);
                }
                Err(err) => {
                    let reply = llsd_map! {
                        "error" => err,
                    };
                    send_reply(app, reply_pump, reply, data);
                }
            }
        }
        "toggle" => {
            let key = data.get("key").as_str();
            if key.is_empty() {
                let reply = llsd_map! { "error" => "Missing required 'key' argument" };
                send_reply(app, reply_pump, reply, data);
                return;
            }

            match toggle_setting(app, key) {
                Ok(new_val) => {
                    let reply = llsd_map! {
                        "key" => key,
                        "status" => true,
                        "value" => new_val,
                    };
                    send_reply(app, reply_pump, reply, data);
                }
                Err(err) => {
                    let reply = llsd_map! {
                        "error" => err,
                    };
                    send_reply(app, reply_pump, reply, data);
                }
            }
        }
        "groups" => {
            let reply = llsd_map! {
                "groups" => Llsd::Array(vec![
                    Llsd::String("Display".to_string()),
                    Llsd::String("Graphics".to_string()),
                    Llsd::String("Audio".to_string()),
                    Llsd::String("Network".to_string()),
                    Llsd::String("Chat".to_string()),
                    Llsd::String("General".to_string()),
                ]),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "vars" => {
            let mut vars_map = Map::new();
            vars_map.insert("DrawDistance".to_string(), llsd_map! { "type" => "real", "desc" => "Viewer draw distance in meters" });
            vars_map.insert("FPSLimit".to_string(), llsd_map! { "type" => "integer", "desc" => "Maximum FPS limit" });
            vars_map.insert("VSync".to_string(), llsd_map! { "type" => "boolean", "desc" => "Vertical synchronization" });
            vars_map.insert("Shadows".to_string(), llsd_map! { "type" => "boolean", "desc" => "Real-time shadow rendering" });
            vars_map.insert("ShowHUDs".to_string(), llsd_map! { "type" => "boolean", "desc" => "Visibility of attached HUDs" });
            vars_map.insert("Language".to_string(), llsd_map! { "type" => "string", "desc" => "Viewer user interface language code" });
            vars_map.insert("Username".to_string(), llsd_map! { "type" => "string", "desc" => "Default account username" });
            vars_map.insert("AutoHoverHeight".to_string(), llsd_map! { "type" => "boolean", "desc" => "Automatic avatar hover height adjustment" });
            vars_map.insert("OcclusionCulling".to_string(), llsd_map! { "type" => "boolean", "desc" => "GPU occlusion culling" });
            vars_map.insert("TextureBudgetMB".to_string(), llsd_map! { "type" => "integer", "desc" => "Texture video memory budget in MB" });
            vars_map.insert("UIScale".to_string(), llsd_map! { "type" => "real", "desc" => "User interface scale factor" });
            vars_map.insert("Glow".to_string(), llsd_map! { "type" => "boolean", "desc" => "Glow rendering" });
            vars_map.insert("Bloom".to_string(), llsd_map! { "type" => "boolean", "desc" => "Bloom post-processing" });
            let reply = llsd_map! {
                "vars" => Llsd::Map(vars_map),
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLViewerControl"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_chat(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "sendChat" | "send" | "chat" => {
            let message = if data.has("message") {
                data.get("message").as_str()
            } else {
                data.get("text").as_str()
            };
            let channel = if data.has("channel") {
                data.get("channel").as_i32()
            } else {
                0
            };
            let chat_type = if data.has("type") {
                match data.get("type") {
                    Llsd::Integer(i) => match i {
                        0 => ChatType::Whisper,
                        2 => ChatType::Shout,
                        _ => ChatType::Normal,
                    },
                    Llsd::String(s) => match s.to_ascii_lowercase().as_str() {
                        "whisper" => ChatType::Whisper,
                        "shout" => ChatType::Shout,
                        _ => ChatType::Normal,
                    },
                    _ => ChatType::Normal,
                }
            } else {
                ChatType::Normal
            };

            app.net.send(NetCommand::Chat {
                message: message.to_string(),
                channel,
                chat_type,
            });

            let reply = llsd_map! {
                "status" => true,
                "message" => message,
                "channel" => channel,
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLChat"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_agent(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "get" | "getAgent" | "get_agent" | "getPosition" | "get_position" => {
            let pos = app.world.agent.position;
            let region_name = app.world.region_name();
            let slurl = format!("secondlife://{}/{:.0}/{:.0}/{:.0}", region_name, pos.x, pos.y, pos.z);
            let mut reply_map = llsd_map! {
                "position" => Llsd::Array(vec![
                    Llsd::Real(pos.x as f64),
                    Llsd::Real(pos.y as f64),
                    Llsd::Real(pos.z as f64),
                ]),
                "x" => pos.x as f64,
                "y" => pos.y as f64,
                "z" => pos.z as f64,
                "yaw" => app.world.agent.yaw as f64,
                "flying" => app.world.agent.flying,
                "region" => region_name.clone(),
                "region_name" => region_name,
                "slurl" => slurl.clone(),
                "last_slurl" => slurl,
            };
            if let Some((hx, hy, hz)) = app.world.map.home {
                reply_map.insert("home_global_x".to_string(), Llsd::Real(hx));
                reply_map.insert("home_global_y".to_string(), Llsd::Real(hy));
                reply_map.insert("home_z".to_string(), Llsd::Real(hz as f64));
            }
            send_reply(app, reply_pump, reply_map, data);
        }
        "getHome" | "get_home" => {
            let mut reply = llsd_map! {
                "status" => true,
            };
            if let Some((hx, hy, hz)) = app.world.map.home {
                reply.insert("has_home".to_string(), Llsd::Boolean(true));
                reply.insert("home_x".to_string(), Llsd::Real(hx % 256.0));
                reply.insert("home_y".to_string(), Llsd::Real(hy % 256.0));
                reply.insert("home_z".to_string(), Llsd::Real(hz as f64));
                reply.insert("global_x".to_string(), Llsd::Real(hx));
                reply.insert("global_y".to_string(), Llsd::Real(hy));
            } else {
                reply.insert("has_home".to_string(), Llsd::Boolean(false));
            }
            send_reply(app, reply_pump, reply, data);
        }
        "getID" | "get_id" => {
            let id = app.world.agent_id.to_string();
            let reply = llsd_map! {
                "id" => id.clone(),
                "agent_id" => id,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "move" | "step" => {
            let raw_step = data.get("step").as_f64();
            let step = (if raw_step == 0.0 { 2.0_f64 } else { raw_step }) as f32;
            let dir = data.get("direction").as_str();

            let (sin_yaw, cos_yaw) = app.world.agent.yaw.sin_cos();
            let forward = glam::Vec3::new(cos_yaw, sin_yaw, 0.0);
            let left = glam::Vec3::new(-sin_yaw, cos_yaw, 0.0);

            match dir {
                "forward" | "up_arrow" | "w" => {
                    let target = app.world.agent.position + forward * step;
                    app.leap_walk_to(target, false);
                }
                "backward" | "down_arrow" | "s" => {
                    let target = app.world.agent.position - forward * step;
                    app.leap_walk_to(target, false);
                }
                "left" | "strafe_left" | "q" => {
                    let target = app.world.agent.position + left * step;
                    app.leap_walk_to(target, false);
                }
                "right" | "strafe_right" | "e" => {
                    let target = app.world.agent.position - left * step;
                    app.leap_walk_to(target, false);
                }
                "turn_left" | "a" => {
                    app.world.agent.yaw += 0.2;
                }
                "turn_right" | "d" => {
                    app.world.agent.yaw -= 0.2;
                }
                "up" | "jump" | "page_up" => {
                    if app.world.agent.flying {
                        let target = app.world.agent.position + glam::Vec3::new(0.0, 0.0, step);
                        app.leap_walk_to(target, false);
                    } else {
                        app.world.agent.record_jump_input(std::time::Instant::now());
                    }
                }
                "down" | "crouch" | "page_down" => {
                    if app.world.agent.flying {
                        let target = app.world.agent.position - glam::Vec3::new(0.0, 0.0, step);
                        app.leap_walk_to(target, false);
                    }
                }
                "fly" | "toggle_fly" => {
                    app.toggle_fly();
                }
                "stop" => {
                    app.stop_autopilot(true);
                    app.controls_held.clear();
                }
                _ => {
                    let dx = data.get("dx").as_f64() as f32;
                    let dy = data.get("dy").as_f64() as f32;
                    let dz = data.get("dz").as_f64() as f32;
                    let target = app.world.agent.position + glam::Vec3::new(dx, dy, dz);
                    app.leap_walk_to(target, false);
                }
            }

            let pos = app.world.agent.position;
            let region_name = app.world.region_name();
            let slurl = format!("secondlife://{}/{:.0}/{:.0}/{:.0}", region_name, pos.x, pos.y, pos.z);
            let reply = llsd_map! {
                "status" => true,
                "x" => pos.x as f64,
                "y" => pos.y as f64,
                "z" => pos.z as f64,
                "yaw" => app.world.agent.yaw as f64,
                "flying" => app.world.agent.flying,
                "region" => region_name,
                "slurl" => slurl,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "start_move" | "key_down" => {
            let key = data.get("direction").as_str();
            let key = if key.is_empty() { data.get("key").as_str() } else { key };
            let act = match key {
                "forward" | "up_arrow" | "w" => Some(crate::keybinds::Action::Forward),
                "backward" | "down_arrow" | "s" => Some(crate::keybinds::Action::Back),
                "left" | "turn_left" | "a" => Some(crate::keybinds::Action::Left),
                "right" | "turn_right" | "d" => Some(crate::keybinds::Action::Right),
                "strafe_left" | "q" => Some(crate::keybinds::Action::StrafeLeft),
                "strafe_right" | "e" => Some(crate::keybinds::Action::StrafeRight),
                "up" | "jump" | "page_up" => Some(crate::keybinds::Action::Up),
                "down" | "crouch" | "page_down" => Some(crate::keybinds::Action::Down),
                _ => None,
            };
            if let Some(action) = act {
                if !app.controls_held.contains(&action) {
                    app.controls_held.push(action);
                }
            }
            let reply = llsd_map! { "status" => true };
            send_reply(app, reply_pump, reply, data);
        }
        "stop_move" | "key_up" => {
            let key = data.get("direction").as_str();
            let key = if key.is_empty() { data.get("key").as_str() } else { key };
            let act = match key {
                "forward" | "up_arrow" | "w" => Some(crate::keybinds::Action::Forward),
                "backward" | "down_arrow" | "s" => Some(crate::keybinds::Action::Back),
                "left" | "turn_left" | "a" => Some(crate::keybinds::Action::Left),
                "right" | "turn_right" | "d" => Some(crate::keybinds::Action::Right),
                "strafe_left" | "q" => Some(crate::keybinds::Action::StrafeLeft),
                "strafe_right" | "e" => Some(crate::keybinds::Action::StrafeRight),
                "up" | "jump" | "page_up" => Some(crate::keybinds::Action::Up),
                "down" | "crouch" | "page_down" => Some(crate::keybinds::Action::Down),
                _ => None,
            };
            if let Some(action) = act {
                app.controls_held.retain(|a| *a != action);
            } else if key == "all" || key.is_empty() {
                app.controls_held.clear();
            }
            let reply = llsd_map! { "status" => true };
            send_reply(app, reply_pump, reply, data);
        }
        "stop" | "stop_autopilot" => {
            app.stop_autopilot(true);
            app.controls_held.clear();
            let reply = llsd_map! {
                "status" => true,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "teleport" => {
            let slurl = data.get("slurl").as_str();
            let dest = data.get("dest").as_str();
            let text_target = if !slurl.is_empty() { slurl } else { dest };

            if !text_target.is_empty() {
                app.teleport_to_text(text_target);
            } else {
                let region = data.get("region").as_str();
                let x = data.get("x").as_f64() as f32;
                let y = data.get("y").as_f64() as f32;
                let z = {
                    let v = data.get("z").as_f64();
                    if v == 0.0 { 25.0_f64 } else { v }
                } as f32;
                let target_region = if region.is_empty() { app.world.region_name() } else { region.to_string() };
                app.teleport_to_location(target_region, glam::Vec3::new(x, y, z));
            }

            let reply = llsd_map! {
                "status" => true,
                "message" => "Teleport initiated",
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLAgent"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_login(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "getState" | "status" => {
            let state_str = match app.screen {
                Screen::Login => "STATE_LOGIN_SHOW",
                Screen::Loading { .. } => "STATE_LOGIN_WAIT",
                Screen::World => "STATE_STARTED",
            };
            let reply = llsd_map! {
                "state" => state_str,
                "logged_in" => app.world.login.is_some(),
                "agent_id" => app.world.agent_id.to_string(),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "login" | "authenticate" => {
            let username = data.get("username").as_str();
            let first = data.get("first").as_str();
            let last = data.get("last").as_str();
            if !username.is_empty() {
                app.settings.username = username.to_string();
            } else if !first.is_empty() && !last.is_empty() {
                app.settings.username = format!("{first} {last}");
            }
            let pass = data.get("password").as_str();
            if !pass.is_empty() {
                app.login_form.password = pass.to_string();
            }
            let uri = data.get("uri").as_str();
            if !uri.is_empty() {
                app.settings.grid = crate::login_accounts::grid(uri);
                // Also store the custom URI if it's not a known grid
                if matches!(app.settings.grid, crate::settings::GridChoice::Custom) {
                    app.settings.custom_login_uri = uri.to_string();
                }
            }
            let start = data.get("start").as_str();
            if !start.is_empty() {
                app.login_form.start_slurl = crate::slurl::parse(start);
            }
            app.start_login();
            let reply = llsd_map! {
                "status" => true,
                "state" => "STATE_LOGIN_WAIT",
                "message" => "Login initiation dispatched",
            };
            send_reply(app, reply_pump, reply, data);
        }
        "logout" => {
            app.net.send(NetCommand::Logout);
            let reply = llsd_map! { "status" => true };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLLogin"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_camera(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "get" | "getCamera" | "get_camera" | "getPosition" | "get_position" | "query" | "" => {
            let pos = app.camera.position;
            let target = app.camera.target;
            let fov_rad = app.camera.fov_y as f64;
            let fov_deg = fov_rad.to_degrees();
            let mut pos_map = Map::new();
            pos_map.insert("x".to_string(), Llsd::Real(pos.x as f64));
            pos_map.insert("y".to_string(), Llsd::Real(pos.y as f64));
            pos_map.insert("z".to_string(), Llsd::Real(pos.z as f64));
            let mut tgt_map = Map::new();
            tgt_map.insert("x".to_string(), Llsd::Real(target.x as f64));
            tgt_map.insert("y".to_string(), Llsd::Real(target.y as f64));
            tgt_map.insert("z".to_string(), Llsd::Real(target.z as f64));
            let reply = llsd_map! {
                "status" => true,
                "is_manual" => app.camera.is_manual_view(),
                "position" => Llsd::Array(vec![
                    Llsd::Real(pos.x as f64),
                    Llsd::Real(pos.y as f64),
                    Llsd::Real(pos.z as f64),
                ]),
                "position_map" => Llsd::Map(pos_map),
                "camera_pos" => Llsd::Array(vec![
                    Llsd::Real(pos.x as f64),
                    Llsd::Real(pos.y as f64),
                    Llsd::Real(pos.z as f64),
                ]),
                "x" => pos.x as f64,
                "y" => pos.y as f64,
                "z" => pos.z as f64,
                "target" => Llsd::Array(vec![
                    Llsd::Real(target.x as f64),
                    Llsd::Real(target.y as f64),
                    Llsd::Real(target.z as f64),
                ]),
                "target_map" => Llsd::Map(tgt_map),
                "camera_target" => Llsd::Array(vec![
                    Llsd::Real(target.x as f64),
                    Llsd::Real(target.y as f64),
                    Llsd::Real(target.z as f64),
                ]),
                "target_x" => target.x as f64,
                "target_y" => target.y as f64,
                "target_z" => target.z as f64,
                "fov" => fov_deg,
                "fov_deg" => fov_deg,
                "fov_rad" => fov_rad,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "set" | "set_position" | "setPosition" => {
            let mut pos: Option<glam::Vec3> = None;
            let mut target: Option<glam::Vec3> = None;
            let mut fov: Option<f32> = None;

            if data.has("position") {
                let v = data.get("position");
                if v.is_array() && v.len() >= 3 {
                    let arr = v.as_vec3();
                    pos = Some(glam::Vec3::new(arr[0], arr[1], arr[2]));
                } else if v.is_map() {
                    let x = v.get("x").as_f32();
                    let y = v.get("y").as_f32();
                    let z = v.get("z").as_f32();
                    pos = Some(glam::Vec3::new(x, y, z));
                }
            } else if data.has("x") && data.has("y") && data.has("z") {
                let x = data.get("x").as_f32();
                let y = data.get("y").as_f32();
                let z = data.get("z").as_f32();
                pos = Some(glam::Vec3::new(x, y, z));
            }

            if data.has("target") {
                let v = data.get("target");
                if v.is_array() && v.len() >= 3 {
                    let arr = v.as_vec3();
                    target = Some(glam::Vec3::new(arr[0], arr[1], arr[2]));
                } else if v.is_map() {
                    let tx = v.get("x").as_f32();
                    let ty = v.get("y").as_f32();
                    let tz = v.get("z").as_f32();
                    target = Some(glam::Vec3::new(tx, ty, tz));
                }
            } else if data.has("target_x") && data.has("target_y") && data.has("target_z") {
                let tx = data.get("target_x").as_f32();
                let ty = data.get("target_y").as_f32();
                let tz = data.get("target_z").as_f32();
                target = Some(glam::Vec3::new(tx, ty, tz));
            }

            if data.has("fov") {
                let raw_fov = data.get("fov").as_f32();
                let fov_rad = if raw_fov > 3.2 { raw_fov.to_radians() } else { raw_fov };
                fov = Some(fov_rad.clamp(0.1, std::f32::consts::PI - 0.1));
            }

            let updated = pos.is_some() || target.is_some() || fov.is_some();
            if updated {
                app.camera.set_manual_view(pos, target, fov);
                if let Some(gfx) = &mut app.gfx {
                    gfx.window.request_redraw();
                }
            }

            let reply = llsd_map! {
                "status" => updated,
                "position" => Llsd::Array(vec![
                    Llsd::Real(app.camera.position.x as f64),
                    Llsd::Real(app.camera.position.y as f64),
                    Llsd::Real(app.camera.position.z as f64),
                ]),
                "target" => Llsd::Array(vec![
                    Llsd::Real(app.camera.target.x as f64),
                    Llsd::Real(app.camera.target.y as f64),
                    Llsd::Real(app.camera.target.z as f64),
                ]),
                "fov" => (app.camera.fov_y as f64).to_degrees(),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "look_at" | "lookAt" | "setTarget" | "set_target" => {
            let mut target: Option<glam::Vec3> = None;
            if data.has("target") {
                let v = data.get("target");
                if v.is_array() && v.len() >= 3 {
                    let arr = v.as_vec3();
                    target = Some(glam::Vec3::new(arr[0], arr[1], arr[2]));
                } else if v.is_map() {
                    let tx = v.get("x").as_f32();
                    let ty = v.get("y").as_f32();
                    let tz = v.get("z").as_f32();
                    target = Some(glam::Vec3::new(tx, ty, tz));
                }
            } else if data.has("target_x") && data.has("target_y") && data.has("target_z") {
                let tx = data.get("target_x").as_f32();
                let ty = data.get("target_y").as_f32();
                let tz = data.get("target_z").as_f32();
                target = Some(glam::Vec3::new(tx, ty, tz));
            } else if data.has("x") && data.has("y") && data.has("z") {
                let tx = data.get("x").as_f32();
                let ty = data.get("y").as_f32();
                let tz = data.get("z").as_f32();
                target = Some(glam::Vec3::new(tx, ty, tz));
            }
            let updated = target.is_some();
            if updated {
                app.camera.set_manual_view(None, target, None);
                if let Some(gfx) = &mut app.gfx {
                    gfx.window.request_redraw();
                }
            }
            let reply = llsd_map! {
                "status" => updated,
                "target" => Llsd::Array(vec![
                    Llsd::Real(app.camera.target.x as f64),
                    Llsd::Real(app.camera.target.y as f64),
                    Llsd::Real(app.camera.target.z as f64),
                ]),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "look_at_avatar" | "lookAtAvatar" => {
            let agent_head = app.world.agent.position + glam::Vec3::Z * 1.0;
            app.camera.set_manual_view(None, Some(agent_head), None);
            if let Some(gfx) = &mut app.gfx {
                gfx.window.request_redraw();
            }
            let reply = llsd_map! {
                "status" => true,
                "target" => Llsd::Array(vec![
                    Llsd::Real(agent_head.x as f64),
                    Llsd::Real(agent_head.y as f64),
                    Llsd::Real(agent_head.z as f64),
                ]),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "orbit" => {
            let yaw_deg = if data.has("yaw_deg") {
                data.get("yaw_deg").as_f32()
            } else if data.has("yaw") {
                data.get("yaw").as_f32()
            } else {
                0.0
            };
            let pitch_deg = if data.has("pitch_deg") {
                data.get("pitch_deg").as_f32()
            } else if data.has("pitch") {
                data.get("pitch").as_f32()
            } else {
                0.0
            };

            let cx = app.camera.position.x;
            let cy = app.camera.position.y;
            let cz = app.camera.position.z;
            let tx = app.camera.target.x;
            let ty = app.camera.target.y;
            let tz = app.camera.target.z;

            let rx = cx - tx;
            let ry = cy - ty;
            let rz = cz - tz;
            let dist = (rx * rx + ry * ry + rz * rz).sqrt().max(0.1);
            let current_yaw = ry.atan2(rx);
            let current_pitch = (rz / dist).asin();
            let new_yaw = current_yaw + yaw_deg.to_radians();
            let new_pitch = (current_pitch + pitch_deg.to_radians()).clamp(-1.45, 1.45);
            let new_rx = dist * new_pitch.cos() * new_yaw.cos();
            let new_ry = dist * new_pitch.cos() * new_yaw.sin();
            let new_rz = dist * new_pitch.sin();

            let new_cam = glam::Vec3::new(tx + new_rx, ty + new_ry, tz + new_rz);
            app.camera.set_manual_view(Some(new_cam), Some(app.camera.target), None);
            if let Some(gfx) = &mut app.gfx {
                gfx.window.request_redraw();
            }

            let reply = llsd_map! {
                "status" => true,
                "position" => Llsd::Array(vec![
                    Llsd::Real(new_cam.x as f64),
                    Llsd::Real(new_cam.y as f64),
                    Llsd::Real(new_cam.z as f64),
                ]),
                "target" => Llsd::Array(vec![
                    Llsd::Real(tx as f64),
                    Llsd::Real(ty as f64),
                    Llsd::Real(tz as f64),
                ]),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "pan" => {
            let fwd = if data.has("forward") { data.get("forward").as_f32() } else { 0.0 };
            let strafe = if data.has("strafe") { data.get("strafe").as_f32() } else { 0.0 };
            let elev = if data.has("elevation") { data.get("elevation").as_f32() } else { 0.0 };

            let c = app.camera.position;
            let t = app.camera.target;
            let dir = (t - c).normalize_or(glam::Vec3::X);
            let right = glam::Vec3::new(dir.y, -dir.x, 0.0).normalize_or(glam::Vec3::Y);
            let delta = dir * fwd + right * strafe + glam::Vec3::Z * elev;

            let new_pos = c + delta;
            let new_target = t + delta;
            app.camera.set_manual_view(Some(new_pos), Some(new_target), None);
            if let Some(gfx) = &mut app.gfx {
                gfx.window.request_redraw();
            }

            let reply = llsd_map! {
                "status" => true,
                "position" => Llsd::Array(vec![
                    Llsd::Real(new_pos.x as f64),
                    Llsd::Real(new_pos.y as f64),
                    Llsd::Real(new_pos.z as f64),
                ]),
                "target" => Llsd::Array(vec![
                    Llsd::Real(new_target.x as f64),
                    Llsd::Real(new_target.y as f64),
                    Llsd::Real(new_target.z as f64),
                ]),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "preset" | "set_preset" | "framing_preset" => {
            let preset_name = if data.has("preset") {
                data.get("preset").as_str()
            } else if data.has("name") {
                data.get("name").as_str()
            } else {
                "portrait"
            };

            let ax = app.world.agent.position.x;
            let ay = app.world.agent.position.y;
            let az = app.world.agent.position.z;
            let yaw = app.world.agent.yaw;

            let (cam, tgt, fov) = match preset_name {
                // Front Views (facing the avatar from the front)
                "front_portrait" | "portrait" => {
                    let dist = 1.6_f32;
                    let cx = ax + dist * yaw.cos();
                    let cy = ay + dist * yaw.sin();
                    (glam::Vec3::new(cx, cy, az + 1.2), glam::Vec3::new(ax, ay, az + 1.2), 40.0_f32.to_radians())
                }
                "front_medium" | "front_torso" => {
                    let dist = 2.6_f32;
                    let cx = ax + dist * yaw.cos();
                    let cy = ay + dist * yaw.sin();
                    (glam::Vec3::new(cx, cy, az + 1.0), glam::Vec3::new(ax, ay, az + 0.9), 50.0_f32.to_radians())
                }
                "front_full_body" | "front_body" => {
                    let dist = 3.8_f32;
                    let cx = ax + dist * yaw.cos();
                    let cy = ay + dist * yaw.sin();
                    (glam::Vec3::new(cx, cy, az + 0.7), glam::Vec3::new(ax, ay, az + 0.5), 55.0_f32.to_radians())
                }
                "front_left_angle" | "front_three_quarter_left" => {
                    let angle = yaw + 45.0_f32.to_radians();
                    let dist = 2.4_f32;
                    let cx = ax + dist * angle.cos();
                    let cy = ay + dist * angle.sin();
                    (glam::Vec3::new(cx, cy, az + 1.1), glam::Vec3::new(ax, ay, az + 1.0), 45.0_f32.to_radians())
                }
                "front_right_angle" | "front_three_quarter_right" => {
                    let angle = yaw - 45.0_f32.to_radians();
                    let dist = 2.4_f32;
                    let cx = ax + dist * angle.cos();
                    let cy = ay + dist * angle.sin();
                    (glam::Vec3::new(cx, cy, az + 1.1), glam::Vec3::new(ax, ay, az + 1.0), 45.0_f32.to_radians())
                }
                // Rear Views (behind the avatar, looking forward)
                "rear_third_person" | "rear_follow" | "full_body" => {
                    let dist = 3.8_f32;
                    let cx = ax - dist * yaw.cos();
                    let cy = ay - dist * yaw.sin();
                    (glam::Vec3::new(cx, cy, az + 0.8), glam::Vec3::new(ax, ay, az + 0.5), 55.0_f32.to_radians())
                }
                "rear_portrait" | "rear_over_shoulder" => {
                    let dist = 1.6_f32;
                    let cx = ax - dist * yaw.cos();
                    let cy = ay - dist * yaw.sin();
                    (glam::Vec3::new(cx, cy, az + 1.2), glam::Vec3::new(ax + 2.0 * yaw.cos(), ay + 2.0 * yaw.sin(), az + 1.2), 40.0_f32.to_radians())
                }
                "wide_landscape" | "rear_wide_landscape" => {
                    let dist = 14.0_f32;
                    let cx = ax - dist * yaw.cos();
                    let cy = ay - dist * yaw.sin();
                    (glam::Vec3::new(cx, cy, az + 4.5), glam::Vec3::new(ax, ay, az + 1.0), 80.0_f32.to_radians())
                }
                // Profile Views
                "profile_left" => {
                    let dist = 2.5_f32;
                    let cx = ax - dist * yaw.sin();
                    let cy = ay + dist * yaw.cos();
                    (glam::Vec3::new(cx, cy, az + 1.1), glam::Vec3::new(ax, ay, az + 1.1), 45.0_f32.to_radians())
                }
                "profile_right" => {
                    let dist = 2.5_f32;
                    let cx = ax + dist * yaw.sin();
                    let cy = ay - dist * yaw.cos();
                    (glam::Vec3::new(cx, cy, az + 1.1), glam::Vec3::new(ax, ay, az + 1.1), 45.0_f32.to_radians())
                }
                // Overhead / Top-down
                "overhead" | "birds_eye" => {
                    (glam::Vec3::new(ax, ay, az + 35.0), glam::Vec3::new(ax, ay, az), 75.0_f32.to_radians())
                }
                _ => (app.camera.position, app.camera.target, app.camera.fov_y),
            };

            app.camera.set_manual_view(Some(cam), Some(tgt), Some(fov));
            if let Some(gfx) = &mut app.gfx {
                gfx.window.request_redraw();
            }

            let reply = llsd_map! {
                "status" => true,
                "preset" => preset_name,
                "position" => Llsd::Array(vec![
                    Llsd::Real(cam.x as f64),
                    Llsd::Real(cam.y as f64),
                    Llsd::Real(cam.z as f64),
                ]),
                "target" => Llsd::Array(vec![
                    Llsd::Real(tgt.x as f64),
                    Llsd::Real(tgt.y as f64),
                    Llsd::Real(tgt.z as f64),
                ]),
                "fov" => (fov as f64).to_degrees(),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "reset" => {
            app.camera.clear_manual_view();
            app.camera.reset_to_defaults(&mut app.world.agent, &app.settings.camera);
            if let Some(gfx) = &mut app.gfx {
                gfx.window.request_redraw();
            }
            let reply = llsd_map! { "status" => true };
            send_reply(app, reply_pump, reply, data);
        }
        "takePicture" | "snapshot" | "screenshot" => {
            if !app.in_world() {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => "Cannot capture snapshot: avatar is not logged into the world (screen is not active)",
                };
                send_reply(app, reply_pump, reply, data);
                return;
            }
            if matches!(app.screen, Screen::Loading { .. }) {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => "Cannot capture snapshot: world is currently loading",
                };
                send_reply(app, reply_pump, reply, data);
                return;
            }

            let raw_w = data.get("width").as_i32();
            let raw_h = data.get("height").as_i32();
            let width = if raw_w <= 0 { 0 } else { (raw_w as u32).clamp(160, 3840) };
            let height = if raw_h <= 0 { 0 } else { (raw_h as u32).clamp(120, 2160) };
            let save_to = data.get("save_to").as_str();

            let capture_ui = if data.has("ui") {
                data.get("ui").as_bool()
            } else if data.has("interface") {
                data.get("interface").as_bool()
            } else {
                true
            };

            if let Some(gfx) = &mut app.gfx {
                if capture_ui {
                    gfx.renderer.capture_request = true;
                } else {
                    gfx.renderer.capture_scene = true;
                }
                gfx.window.request_redraw();
            }

            app.pending_leap_snapshot = Some(PendingLeapSnapshot {
                reply_pump: reply_pump.to_string(),
                request: data.clone(),
                width,
                height,
                save_to: if save_to.is_empty() { None } else { Some(save_to.to_string()) },
                ui: capture_ui,
                requested_at: Instant::now(),
            });
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLCamera"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

/// Pending snapshot request queued by LEAP, waiting for the GPU frame readback.
#[derive(Debug, Clone)]
pub struct PendingLeapSnapshot {
    pub reply_pump: String,
    pub request: Llsd,
    pub width: u32,
    pub height: u32,
    pub save_to: Option<String>,
    pub ui: bool,
    pub requested_at: Instant,
}

/// Called once the GPU frame render has been read back into main memory.
pub fn finish_leap_snapshot(
    app: &App,
    snap: PendingLeapSnapshot,
    frame_w: u32,
    frame_h: u32,
    mut px: Vec<u8>,
) {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use std::io::Cursor;

    // Ensure all pixels have alpha = 255 (fully opaque) so the saved PNG is never transparent or invisible.
    for chunk in px.chunks_exact_mut(4) {
        chunk[3] = 255;
    }

    let img = match image::RgbaImage::from_raw(frame_w, frame_h, px) {
        Some(img) => img,
        None => {
            fail_leap_snapshot(app, snap, "Failed to construct RGBA image from GPU framebuffer readback");
            return;
        }
    };

    let (final_w, final_h, final_img) = if snap.width > 0 && snap.height > 0 && (snap.width != frame_w || snap.height != frame_h) {
        let resized = image::imageops::resize(&img, snap.width, snap.height, image::imageops::FilterType::Triangle);
        (snap.width, snap.height, resized)
    } else {
        (frame_w, frame_h, img)
    };

    let mut png_bytes = Vec::new();
    let mut cursor = Cursor::new(&mut png_bytes);
    if let Err(e) = final_img.write_to(&mut cursor, image::ImageFormat::Png) {
        fail_leap_snapshot(app, snap, &format!("PNG encode error: {e}"));
        return;
    }

    let custom_target = snap.save_to.clone();
    let file_path = if let Some(custom) = custom_target {
        if let Some(parent) = std::path::Path::new(&custom).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Err(e) = std::fs::write(&custom, &png_bytes) {
            fail_leap_snapshot(app, snap, &format!("Failed to save snapshot to {custom}: {e}"));
            return;
        }
        custom
    } else {
        let dir = std::path::PathBuf::from("snapshots");
        let _ = std::fs::create_dir_all(&dir);
        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
        let path = dir.join(format!("aurora_snapshot_{timestamp}.png"));
        let path_str = path.to_string_lossy().to_string();
        if let Err(e) = std::fs::write(&path, &png_bytes) {
            log::warn!("Failed to write snapshot to {}: {e}", path.display());
        }
        path_str
    };

    let png_base64 = STANDARD.encode(&png_bytes);
    let pos = app.world.agent.position;
    let reply = llsd_map! {
        "status" => true,
        "width" => final_w as i32,
        "height" => final_h as i32,
        "file_path" => file_path,
        "png_base64" => png_base64,
        "position" => format!("{:.1}, {:.1}, {:.1}", pos.x, pos.y, pos.z),
    };
    send_reply(app, &snap.reply_pump, reply, &snap.request);
}

/// Called if a snapshot request timed out or encountered an error.
pub fn fail_leap_snapshot(app: &App, snap: PendingLeapSnapshot, err: &str) {
    let reply = llsd_map! {
        "status" => false,
        "error" => err.to_string(),
    };
    send_reply(app, &snap.reply_pump, reply, &snap.request);
}

fn handle_inventory(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "getRoot" | "fetch_root" => {
            let reply = llsd_map! {
                "root" => app.world.inventory.root.to_string(),
                "owner" => app.world.inventory.owner.to_string(),
                "lib_root" => app.world.inventory.lib_root.to_string(),
                "lib_owner" => app.world.inventory.lib_owner.to_string(),
                "folder_count" => app.world.inventory.folders.len() as i32,
                "item_count" => app.world.inventory.items.len() as i32,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "getTree" | "fetch_inventory" | "getDescendants" => {
            let mut folder_list = Vec::new();
            for (id, f) in &app.world.inventory.folders {
                let children_strs: Vec<Llsd> = f.children.iter().map(|c| Llsd::String(c.to_string())).collect();
                let item_strs: Vec<Llsd> = f.items.iter().map(|it| Llsd::String(it.to_string())).collect();
                folder_list.push(llsd_map! {
                    "id" => id.to_string(),
                    "name" => f.info.name.clone(),
                    "parent" => f.info.parent.to_string(),
                    "type_default" => f.info.type_default,
                    "children" => Llsd::Array(children_strs),
                    "items" => Llsd::Array(item_strs),
                    "library" => f.library,
                });
            }
            let mut item_list = Vec::new();
            for (id, item) in &app.world.inventory.items {
                item_list.push(llsd_map! {
                    "id" => id.to_string(),
                    "name" => item.name.clone(),
                    "desc" => item.desc.clone(),
                    "parent" => item.parent.to_string(),
                    "type" => item.asset_type,
                    "inv_type" => item.inv_type,
                    "asset_id" => item.asset_id.to_string(),
                });
            }
            let reply = llsd_map! {
                "status" => true,
                "root" => app.world.inventory.root.to_string(),
                "lib_root" => app.world.inventory.lib_root.to_string(),
                "owner" => app.world.inventory.owner.to_string(),
                "folders" => Llsd::Array(folder_list),
                "items" => Llsd::Array(item_list),
                "folder_count" => app.world.inventory.folders.len() as i32,
                "item_count" => app.world.inventory.items.len() as i32,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "getFolder" | "fetch_folder" => {
            let folder_id_str = data.get("folder_id").as_str();
            let fid = if let Ok(u) = folder_id_str.parse::<Uuid>() {
                u
            } else {
                app.world.inventory.root
            };
            if let Some(f) = app.world.inventory.folders.get(&fid) {
                let children_strs: Vec<Llsd> = f.children.iter().map(|c| Llsd::String(c.to_string())).collect();
                let item_strs: Vec<Llsd> = f.items.iter().map(|it| Llsd::String(it.to_string())).collect();
                let reply = llsd_map! {
                    "status" => true,
                    "id" => fid.to_string(),
                    "name" => f.info.name.clone(),
                    "parent" => f.info.parent.to_string(),
                    "type_default" => f.info.type_default,
                    "children" => Llsd::Array(children_strs),
                    "items" => Llsd::Array(item_strs),
                    "library" => f.library,
                };
                send_reply(app, reply_pump, reply, data);
            } else {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => format!("Folder '{folder_id_str}' not found"),
                };
                send_reply(app, reply_pump, reply, data);
            }
        }
        "getItem" | "get_item" => {
            let item_id_str = data.get("item_id").as_str();
            if let Ok(id) = item_id_str.parse::<Uuid>() {
                if let Some(item) = app.world.inventory.items.get(&id) {
                    let reply = llsd_map! {
                        "item_id" => item.id.to_string(),
                        "name" => item.name.clone(),
                        "desc" => item.desc.clone(),
                        "type" => item.asset_type,
                        "inv_type" => item.inv_type,
                        "parent_id" => item.parent.to_string(),
                        "asset_id" => item.asset_id.to_string(),
                    };
                    send_reply(app, reply_pump, reply, data);
                    return;
                }
            }
            let reply = llsd_map! {
                "error" => format!("Item '{item_id_str}' not found"),
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLInventory"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_world(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "getRegionInfo" | "get_region_info" | "region" | "getRegion" => {
            let in_world = app.in_world();
            if !in_world {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => "Avatar is not active in world",
                };
                send_reply(app, reply_pump, reply, data);
                return;
            }

            let mut reply = llsd_map! {
                "status" => true,
            };

            if let Some(handle) = app.world.main_region {
                reply.insert("handle".to_string(), Llsd::Real(handle as f64));
                if let Some(reg) = app.world.regions.get(&handle) {
                    reply.insert("name".to_string(), Llsd::String(reg.name.clone()));
                    reply.insert("voice_server".to_string(), Llsd::String(reg.voice_server.clone()));
                    reply.insert("max_tasks".to_string(), Llsd::Integer(reg.max_tasks as i32));

                    if let Some(info) = &reg.info {
                        reply.insert("region_id".to_string(), Llsd::String(info.region_id.to_string()));
                        reply.insert("water_height".to_string(), Llsd::Real(info.water_height as f64));
                        reply.insert("sim_access".to_string(), Llsd::Integer(info.sim_access as i32));
                        reply.insert("region_flags".to_string(), Llsd::Integer(info.region_flags as i32));
                        reply.insert("size_x".to_string(), Llsd::Integer(info.size_x as i32));
                        reply.insert("size_y".to_string(), Llsd::Integer(info.size_y as i32));
                        reply.insert("product_name".to_string(), Llsd::String(info.product_name.clone()));
                        reply.insert("owner_id".to_string(), Llsd::String(info.owner.to_string()));
                        reply.insert("is_estate_manager".to_string(), Llsd::Boolean(info.is_estate_manager));
                    }
                }
            }
            send_reply(app, reply_pump, reply, data);
        }
        "getParcelInfo" | "get_parcel_info" | "parcel" | "getParcel" => {
            let in_world = app.in_world();
            if !in_world {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => "Avatar is not active in world",
                };
                send_reply(app, reply_pump, reply, data);
                return;
            }

            let mut reply = llsd_map! {
                "status" => true,
                "parcel_name" => app.world.parcel_name.clone(),
            };

            if let Some(parcel) = &app.world.parcel {
                reply.insert("name".to_string(), Llsd::String(parcel.name.clone()));
                reply.insert("desc".to_string(), Llsd::String(parcel.desc.clone()));
                reply.insert("local_id".to_string(), Llsd::Integer(parcel.local_id));
                reply.insert("owner_id".to_string(), Llsd::String(parcel.owner_id.to_string()));
                reply.insert("group_id".to_string(), Llsd::String(parcel.group_id.to_string()));
                reply.insert("is_group_owned".to_string(), Llsd::Boolean(parcel.is_group_owned));
                reply.insert("area".to_string(), Llsd::Integer(parcel.area));
                reply.insert("claim_price".to_string(), Llsd::Integer(parcel.claim_price));
                reply.insert("rent_price".to_string(), Llsd::Integer(parcel.rent_price));
                reply.insert("status_code".to_string(), Llsd::Integer(parcel.status as i32));
                reply.insert("flags".to_string(), Llsd::Integer(parcel.flags as i32));
                reply.insert("aabb_min".to_string(), Llsd::Array(vec![
                    Llsd::Real(parcel.aabb_min.x as f64),
                    Llsd::Real(parcel.aabb_min.y as f64),
                    Llsd::Real(parcel.aabb_min.z as f64),
                ]));
                reply.insert("aabb_max".to_string(), Llsd::Array(vec![
                    Llsd::Real(parcel.aabb_max.x as f64),
                    Llsd::Real(parcel.aabb_max.y as f64),
                    Llsd::Real(parcel.aabb_max.z as f64),
                ]));
            }
            send_reply(app, reply_pump, reply, data);
        }
        "getSimStats" | "sim_stats" | "stats" => {
            let fps = if app.prev_dt > 0.0 { (1.0 / app.prev_dt) as f64 } else { 0.0 };
            let uptime = app.start.elapsed().as_secs_f64();
            let avatar_count = if app.in_world() { app.world.objects.avatars().count() as i32 } else { 0 };
            let region_count = app.world.regions.len() as i32;
            let main_reg_name = app.world.main_region
                .and_then(|h| app.world.regions.get(&h))
                .map(|r| r.name.clone())
                .unwrap_or_else(|| "None".to_string());

            let reply = llsd_map! {
                "status" => true,
                "fps" => fps,
                "uptime" => uptime,
                "avatar_count" => avatar_count,
                "region_count" => region_count,
                "main_region" => main_reg_name,
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLWorld"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_nearby_avatars(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "getNearby" | "get_nearby" | "nearby" | "radar" => {
            if !app.in_world() {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => "Avatar is not active in world",
                    "avatars" => Llsd::Array(Vec::new()),
                    "count" => 0,
                };
                send_reply(app, reply_pump, reply, data);
                return;
            }

            let my_pos = app.world.agent.position;
            let my_id = app.world.agent_id;
            let max_dist = data.get("max_distance").as_f64();
            let max_dist = if max_dist <= 0.0 { 1024.0 } else { max_dist };

            let mut avatar_entries = Vec::new();

            for (_idx, obj) in app.world.objects.avatars() {
                let dist = (obj.position - my_pos).length() as f64;
                if dist > max_dist {
                    continue;
                }
                let is_self = obj.full_id == my_id;
                let name = if is_self {
                    app.settings.username.clone()
                } else {
                    app.world.avatar_name(&obj.full_id)
                        .or_else(|| obj.display_name())
                        .unwrap_or_else(|| {
                            let id_str = obj.full_id.to_string();
                            format!("Resident {}", &id_str[..id_str.len().min(8)])
                        })
                };

                avatar_entries.push(llsd_map! {
                    "uuid" => obj.full_id.to_string(),
                    "name" => name,
                    "x" => obj.position.x as f64,
                    "y" => obj.position.y as f64,
                    "z" => obj.position.z as f64,
                    "position" => Llsd::Array(vec![
                        Llsd::Real(obj.position.x as f64),
                        Llsd::Real(obj.position.y as f64),
                        Llsd::Real(obj.position.z as f64),
                    ]),
                    "distance" => dist,
                    "is_self" => is_self,
                    "seated" => obj.parent_id != 0,
                    "velocity_x" => obj.velocity.x as f64,
                    "velocity_y" => obj.velocity.y as f64,
                    "velocity_z" => obj.velocity.z as f64,
                });
            }

            // Always ensure self is in the radar list if object store didn't enumerate own avatar yet
            if !avatar_entries.iter().any(|v| v.get("is_self").as_bool()) {
                avatar_entries.push(llsd_map! {
                    "uuid" => my_id.to_string(),
                    "name" => app.settings.username.clone(),
                    "x" => my_pos.x as f64,
                    "y" => my_pos.y as f64,
                    "z" => my_pos.z as f64,
                    "position" => Llsd::Array(vec![
                        Llsd::Real(my_pos.x as f64),
                        Llsd::Real(my_pos.y as f64),
                        Llsd::Real(my_pos.z as f64),
                    ]),
                    "distance" => 0.0,
                    "is_self" => true,
                    "seated" => false,
                    "velocity_x" => 0.0,
                    "velocity_y" => 0.0,
                    "velocity_z" => 0.0,
                });
            }

            // Sort by distance ascending
            avatar_entries.sort_by(|a, b| {
                let da = a.get("distance").as_f64();
                let db = b.get("distance").as_f64();
                da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
            });

            let count = avatar_entries.len() as i32;
            let reply = llsd_map! {
                "status" => true,
                "count" => count,
                "avatars" => Llsd::Array(avatar_entries),
                "agent_x" => my_pos.x as f64,
                "agent_y" => my_pos.y as f64,
                "agent_z" => my_pos.z as f64,
                "agent_yaw" => app.world.agent.yaw as f64,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "getAvatarDetails" | "get_avatar" | "details" => {
            let id_str = data.get("avatar_id").as_str();
            let id_str = if id_str.is_empty() { data.get("uuid").as_str() } else { id_str };
            if let Ok(target_uuid) = id_str.parse::<Uuid>() {
                let my_pos = app.world.agent.position;
                let found = app.world.objects.avatars().find(|(_, obj)| obj.full_id == target_uuid);
                if let Some((_, obj)) = found {
                    let is_self = obj.full_id == app.world.agent_id;
                    let name = if is_self {
                        app.settings.username.clone()
                    } else {
                        app.world.avatar_name(&obj.full_id)
                            .or_else(|| obj.display_name())
                            .unwrap_or_else(|| target_uuid.to_string())
                    };
                    let dist = (obj.position - my_pos).length() as f64;
                    let reply = llsd_map! {
                        "status" => true,
                        "uuid" => obj.full_id.to_string(),
                        "name" => name,
                        "x" => obj.position.x as f64,
                        "y" => obj.position.y as f64,
                        "z" => obj.position.z as f64,
                        "distance" => dist,
                        "is_self" => is_self,
                        "seated" => obj.parent_id != 0,
                        "parent_id" => obj.parent_id as i32,
                        "velocity_x" => obj.velocity.x as f64,
                        "velocity_y" => obj.velocity.y as f64,
                        "velocity_z" => obj.velocity.z as f64,
                    };
                    send_reply(app, reply_pump, reply, data);
                    return;
                }
            }
            let reply = llsd_map! {
                "status" => false,
                "error" => format!("Avatar '{id_str}' not found in simulator radar"),
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLNearbyAvatars"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_objects(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "getNearbyObjects" | "get_nearby" | "nearby" | "list" => {
            if !app.in_world() {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => "Viewer not active in world",
                    "objects" => Llsd::Array(Vec::new()),
                    "count" => 0,
                };
                send_reply(app, reply_pump, reply, data);
                return;
            }

            let my_pos = app.world.agent.position;
            let max_dist = data.get("max_distance").as_f64();
            let max_dist = if max_dist <= 0.0 { 64.0 } else { max_dist };
            let limit = data.get("limit").as_i32();
            let limit = if limit <= 0 { 100 } else { limit as usize };
            let include_attachments = data.get("include_attachments").as_bool();

            let mut object_list = Vec::new();
            for (_idx, obj) in app.world.objects.iter() {
                if obj.is_avatar() {
                    continue;
                }
                if !include_attachments && obj.is_attachment() {
                    continue;
                }
                let dist = (obj.position - my_pos).length() as f64;
                if dist > max_dist {
                    continue;
                }

                let name = if !obj.text.is_empty() {
                    obj.text.clone()
                } else if let Some(n) = obj.display_name() {
                    n
                } else {
                    format!("Primitive #{}", obj.key.local_id)
                };

                object_list.push(llsd_map! {
                    "local_id" => obj.key.local_id as i32,
                    "uuid" => obj.full_id.to_string(),
                    "parent_id" => obj.parent_id as i32,
                    "pcode" => obj.pcode as i32,
                    "name" => name,
                    "x" => obj.position.x as f64,
                    "y" => obj.position.y as f64,
                    "z" => obj.position.z as f64,
                    "position" => Llsd::Array(vec![
                        Llsd::Real(obj.position.x as f64),
                        Llsd::Real(obj.position.y as f64),
                        Llsd::Real(obj.position.z as f64),
                    ]),
                    "scale_x" => obj.scale.x as f64,
                    "scale_y" => obj.scale.y as f64,
                    "scale_z" => obj.scale.z as f64,
                    "distance" => dist,
                    "is_attachment" => obj.is_attachment(),
                    "attachment_point" => obj.attachment_point() as i32,
                    "is_root" => obj.parent_id == 0,
                });
            }

            object_list.sort_by(|a, b| {
                let da = a.get("distance").as_f64();
                let db = b.get("distance").as_f64();
                da.partial_cmp(&db).unwrap_or(std::cmp::Ordering::Equal)
            });

            if object_list.len() > limit {
                object_list.truncate(limit);
            }

            let count = object_list.len() as i32;
            let reply = llsd_map! {
                "status" => true,
                "count" => count,
                "objects" => Llsd::Array(object_list),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "inspect" | "details" => {
            let local_id = data.get("local_id").as_i32() as u32;
            let uuid_str = data.get("uuid").as_str();
            let parsed_uuid = uuid_str.parse::<Uuid>().ok();

            let found = app.world.objects.iter().find(|(_, obj)| {
                (local_id > 0 && obj.key.local_id == local_id)
                    || (parsed_uuid.is_some() && Some(obj.full_id) == parsed_uuid)
            });

            if let Some((_, obj)) = found {
                let my_pos = app.world.agent.position;
                let dist = (obj.position - my_pos).length() as f64;
                let reply = llsd_map! {
                    "status" => true,
                    "local_id" => obj.key.local_id as i32,
                    "uuid" => obj.full_id.to_string(),
                    "parent_id" => obj.parent_id as i32,
                    "pcode" => obj.pcode as i32,
                    "prim_material" => obj.prim_material as i32,
                    "click_action" => obj.click_action as i32,
                    "text" => obj.text.clone(),
                    "name_values" => obj.name_values.clone(),
                    "position" => Llsd::Array(vec![
                        Llsd::Real(obj.position.x as f64),
                        Llsd::Real(obj.position.y as f64),
                        Llsd::Real(obj.position.z as f64),
                    ]),
                    "scale" => Llsd::Array(vec![
                        Llsd::Real(obj.scale.x as f64),
                        Llsd::Real(obj.scale.y as f64),
                        Llsd::Real(obj.scale.z as f64),
                    ]),
                    "velocity" => Llsd::Array(vec![
                        Llsd::Real(obj.velocity.x as f64),
                        Llsd::Real(obj.velocity.y as f64),
                        Llsd::Real(obj.velocity.z as f64),
                    ]),
                    "distance" => dist,
                    "is_attachment" => obj.is_attachment(),
                    "attachment_point" => obj.attachment_point() as i32,
                    "attachment_item_id" => obj.attachment_item_id().map(|u| u.to_string()).unwrap_or_default(),
                };
                send_reply(app, reply_pump, reply, data);
            } else {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => "Object not found in world cache",
                };
                send_reply(app, reply_pump, reply, data);
            }
        }
        "selectObject" | "select" => {
            let local_id = data.get("local_id").as_i32() as u32;
            let uuid_str = data.get("uuid").as_str();
            let parsed_uuid = uuid_str.parse::<Uuid>().ok();

            let found = app.world.objects.iter().find(|(_, obj)| {
                (local_id > 0 && obj.key.local_id == local_id)
                    || (parsed_uuid.is_some() && Some(obj.full_id) == parsed_uuid)
            });

            if let Some((_, obj)) = found {
                let reply = llsd_map! {
                    "status" => true,
                    "selected" => true,
                    "local_id" => obj.key.local_id as i32,
                    "uuid" => obj.full_id.to_string(),
                };
                send_reply(app, reply_pump, reply, data);
            } else {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => "Cannot select: object not found",
                };
                send_reply(app, reply_pump, reply, data);
            }
        }
        "deselect" => {
            let reply = llsd_map! {
                "status" => true,
                "deselected" => true,
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLObjects"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_appearance(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "getWornItems" | "worn" | "get_worn" => {
            let mut worn_attachments = Vec::new();
            for (_idx, obj) in app.world.objects.iter() {
                if obj.is_attachment() {
                    let point = obj.attachment_point();
                    let name = if !obj.text.is_empty() {
                        obj.text.clone()
                    } else if let Some(n) = obj.display_name() {
                        n
                    } else {
                        format!("Attachment #{}", obj.key.local_id)
                    };
                    worn_attachments.push(llsd_map! {
                        "local_id" => obj.key.local_id as i32,
                        "uuid" => obj.full_id.to_string(),
                        "attachment_point" => point as i32,
                        "item_id" => obj.attachment_item_id().map(|u| u.to_string()).unwrap_or_default(),
                        "name" => name,
                    });
                }
            }

            let mut worn_clothing_items = Vec::new();
            for (item_id, wearable_type) in crate::world::appearance::worn_clothing(&app.world.inventory) {
                let name = app.world.inventory.items.get(&item_id).map(|it| it.name.clone()).unwrap_or_else(|| item_id.to_string());
                worn_clothing_items.push(llsd_map! {
                    "item_id" => item_id.to_string(),
                    "wearable_type" => wearable_type as i32,
                    "name" => name,
                });
            }

            let cof_id = crate::world::appearance::cof(&app.world.inventory).map(|u| u.to_string()).unwrap_or_default();

            let reply = llsd_map! {
                "status" => true,
                "cof_id" => cof_id,
                "attachments_count" => worn_attachments.len() as i32,
                "attachments" => Llsd::Array(worn_attachments),
                "clothing_count" => worn_clothing_items.len() as i32,
                "clothing" => Llsd::Array(worn_clothing_items),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "getBakeStatus" | "bake_status" => {
            let reply = llsd_map! {
                "status" => true,
                "baked" => true,
                "avatar_id" => app.world.agent_id.to_string(),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "wearItem" | "wear" => {
            let item_str = data.get("item_id").as_str();
            let item_uuid = if item_str.is_empty() { data.get("uuid").as_str() } else { item_str };
            if let Ok(uuid) = item_uuid.parse::<Uuid>() {
                let replace = data.get("replace").as_bool();
                let point = data.get("attachment_point").as_i32() as u8;
                app.apply_appearance_action(crate::world::appearance::Action::WearItem {
                    item: uuid,
                    replace,
                    point,
                });
                let reply = llsd_map! {
                    "status" => true,
                    "item_id" => uuid.to_string(),
                    "worn" => true,
                };
                send_reply(app, reply_pump, reply, data);
            } else {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => format!("Invalid item UUID '{item_uuid}'"),
                };
                send_reply(app, reply_pump, reply, data);
            }
        }
        "takeOff" | "take_off" | "detach" => {
            let item_str = data.get("item_id").as_str();
            let item_uuid = if item_str.is_empty() { data.get("uuid").as_str() } else { item_str };
            if let Ok(uuid) = item_uuid.parse::<Uuid>() {
                app.apply_appearance_action(crate::world::appearance::Action::Remove(uuid));
                let reply = llsd_map! {
                    "status" => true,
                    "item_id" => uuid.to_string(),
                    "detached" => true,
                };
                send_reply(app, reply_pump, reply, data);
            } else {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => format!("Invalid item UUID '{item_uuid}'"),
                };
                send_reply(app, reply_pump, reply, data);
            }
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLAppearance"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_animation(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "getActiveAnimations" | "get_active" | "active" | "list" => {
            let target_uuid = data.get("avatar_id").as_str().parse::<Uuid>().unwrap_or(app.world.agent_id);
            let active = app.world.animations_of(&target_uuid);
            let mut anim_list = Vec::new();

            for anim in active {
                anim_list.push(llsd_map! {
                    "uuid" => anim.id.to_string(),
                    "sequence" => anim.sequence,
                    "duration_sec" => anim.continuous_start.elapsed().as_secs_f64(),
                });
            }

            let reply = llsd_map! {
                "status" => true,
                "avatar_id" => target_uuid.to_string(),
                "count" => anim_list.len() as i32,
                "animations" => Llsd::Array(anim_list),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "startAnimation" | "start" => {
            let anim_id_str = data.get("anim_id").as_str();
            let anim_id = if anim_id_str.is_empty() { data.get("uuid").as_str() } else { anim_id_str };
            if let Ok(uuid) = anim_id.parse::<Uuid>() {
                let now = Instant::now();
                app.world.animation_preview_clock = Some((uuid, now, now));
                let reply = llsd_map! {
                    "status" => true,
                    "started" => true,
                    "anim_id" => uuid.to_string(),
                };
                send_reply(app, reply_pump, reply, data);
            } else {
                let reply = llsd_map! {
                    "status" => false,
                    "error" => format!("Invalid animation UUID '{anim_id}'"),
                };
                send_reply(app, reply_pump, reply, data);
            }
        }
        "stopAnimation" | "stop" => {
            app.world.animation_preview_clock = None;
            let reply = llsd_map! {
                "status" => true,
                "stopped" => true,
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLAnimation"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_voice(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "getStatus" | "status" | "get" | "" => {
            let reply = llsd_map! {
                "status" => true,
                "voice_enabled" => app.settings.audio.voice_enabled,
                "mic_gain" => app.settings.audio.mic_gain as f64,
                "input_device" => app.settings.audio.input_device.clone(),
                "talking" => app.talking(),
            };
            send_reply(app, reply_pump, reply, data);
        }
        "setMuted" | "mute" => {
            let mute = if data.has("muted") {
                data.get("muted").as_bool()
            } else if data.has("mute") {
                data.get("mute").as_bool()
            } else {
                true
            };
            app.settings.audio.voice_enabled = !mute;
            let reply = llsd_map! {
                "status" => true,
                "muted" => mute,
                "voice_enabled" => app.settings.audio.voice_enabled,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "setVolume" | "setGain" | "set_gain" => {
            let gain = if data.has("volume") {
                data.get("volume").as_f32()
            } else if data.has("gain") {
                data.get("gain").as_f32()
            } else {
                1.0
            };
            app.settings.audio.mic_gain = gain.clamp(0.0, 10.0);
            let reply = llsd_map! {
                "status" => true,
                "mic_gain" => app.settings.audio.mic_gain as f64,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "joinChannel" | "join" => {
            let uri = data.get("uri").as_str();
            let channel_name = data.get("channel").as_str();
            log::info!(target: "leap", "LLVoice: joinChannel uri={uri}, channel={channel_name}");
            let reply = llsd_map! {
                "status" => true,
                "channel_joined" => true,
                "uri" => uri,
            };
            send_reply(app, reply_pump, reply, data);
        }
        "leaveChannel" | "leave" => {
            log::info!(target: "leap", "LLVoice: leaveChannel");
            let reply = llsd_map! {
                "status" => true,
                "channel_left" => true,
            };
            send_reply(app, reply_pump, reply, data);
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLVoice"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

fn handle_leap_listener(app: &mut App, op: &str, data: &Llsd, reply_pump: &str) {
    match op {
        "listen" => {
            let source = data.get("source").as_str();
            let listener = data.get("listener").as_str();
            if source.is_empty() || listener.is_empty() {
                let reply = llsd_map! { "status" => false, "error" => "Missing source or listener argument" };
                send_reply(app, reply_pump, reply, data);
                return;
            }
            if let Some(leap) = &app.leap {
                let tx_outbound = leap.outbound_tx().clone();
                let source_pump = source.to_string();
                let ok = leap.pumps().listen(source, listener, move |evt| {
                    let envelope = aurora_leap::LeapEncoder::encode_message(&source_pump, evt);
                    let _ = tx_outbound.send(envelope);
                    true
                });
                let reply = llsd_map! { "status" => ok };
                send_reply(app, reply_pump, reply, data);
            }
        }
        "stopListening" | "stoplistening" => {
            let source = data.get("source").as_str();
            let listener = data.get("listener").as_str();
            if let Some(leap) = &app.leap {
                let ok = leap.pumps().stop_listening(source, listener);
                let reply = llsd_map! { "status" => ok };
                send_reply(app, reply_pump, reply, data);
            }
        }
        "getListeners" | "list" => {
            let source = data.get("source").as_str();
            if let Some(leap) = &app.leap {
                let listeners: Vec<Llsd> = if source.is_empty() {
                    let mut all = Vec::new();
                    for name in leap.pumps().pump_names() {
                        if let Some(p) = leap.pumps().find(&name) {
                            for l in p.listener_names() {
                                all.push(llsd_map! { "pump" => name.clone(), "listener" => l });
                            }
                        }
                    }
                    all
                } else if let Some(p) = leap.pumps().find(source) {
                    p.listener_names().into_iter().map(Llsd::String).collect()
                } else {
                    Vec::new()
                };
                let reply = llsd_map! {
                    "status" => true,
                    "listeners" => Llsd::Array(listeners),
                };
                send_reply(app, reply_pump, reply, data);
            }
        }
        _ => {
            let reply = llsd_map! {
                "error" => format!("Unknown operation '{op}' for LLLeapListener"),
            };
            send_reply(app, reply_pump, reply, data);
        }
    }
}

// ---------------------------------------------------------------------------
// Settings Helpers
// ---------------------------------------------------------------------------

fn query_setting(settings: &crate::settings::Settings, key: &str) -> Option<Llsd> {
    match key.to_ascii_lowercase().as_str() {
        "drawdistance" | "draw_distance" => Some(Llsd::Real(settings.draw_distance as f64)),
        "fpslimit" | "fps_limit" => Some(Llsd::Integer(settings.fps_limit as i32)),
        "vsync" => Some(Llsd::Boolean(settings.vsync)),
        "shadows" => Some(Llsd::Boolean(settings.shadows)),
        "showhuds" | "show_huds" => Some(Llsd::Boolean(settings.show_huds)),
        "sunbeacon" | "sun_beacon" => Some(Llsd::Boolean(settings.sun_beacon)),
        "moonbeacon" | "moon_beacon" => Some(Llsd::Boolean(settings.moon_beacon)),
        "language" => Some(Llsd::String(settings.language.clone())),
        "username" => Some(Llsd::String(settings.username.clone())),
        "autohoverheight" | "auto_hover_height" => Some(Llsd::Boolean(settings.auto_hover_height)),
        "occlusionculling" | "occlusion_culling" => Some(Llsd::Boolean(settings.occlusion_culling)),
        "texturebudgetmb" | "texture_budget_mb" => Some(Llsd::Integer(settings.texture_budget_mb as i32)),
        "lodfactor" | "lod_factor" => Some(Llsd::Real(settings.lod_factor as f64)),
        "uiscale" | "ui_scale" => Some(Llsd::Real(settings.ui_scale as f64)),
        "glow" => Some(Llsd::Boolean(settings.glow)),
        "bloom" => Some(Llsd::Boolean(settings.bloom)),
        "sharpen" => Some(Llsd::Real(settings.sharpen as f64)),
        "exposure" => Some(Llsd::Real(settings.exposure as f64)),
        _ => None,
    }
}

fn mutate_setting(app: &mut App, key: &str, val: &Llsd) -> Result<Llsd, String> {
    match key.to_ascii_lowercase().as_str() {
        "drawdistance" | "draw_distance" => {
            let v = val.as_f32().clamp(16.0, 1024.0);
            app.settings.draw_distance = v;
            app.net.send(NetCommand::SetDrawDistance(v));
            Ok(Llsd::Real(v as f64))
        }
        "fpslimit" | "fps_limit" => {
            let v = val.as_u32().clamp(10, 500);
            app.settings.fps_limit = v;
            Ok(Llsd::Integer(v as i32))
        }
        "vsync" => {
            let v = val.as_bool();
            app.settings.vsync = v;
            Ok(Llsd::Boolean(v))
        }
        "shadows" => {
            let v = val.as_bool();
            app.settings.shadows = v;
            Ok(Llsd::Boolean(v))
        }
        "showhuds" | "show_huds" => {
            let v = val.as_bool();
            app.settings.show_huds = v;
            Ok(Llsd::Boolean(v))
        }
        "sunbeacon" | "sun_beacon" => {
            let v = val.as_bool();
            app.settings.sun_beacon = v;
            Ok(Llsd::Boolean(v))
        }
        "moonbeacon" | "moon_beacon" => {
            let v = val.as_bool();
            app.settings.moon_beacon = v;
            Ok(Llsd::Boolean(v))
        }
        "language" => {
            let v = val.as_str().to_string();
            app.settings.language = v.clone();
            Ok(Llsd::String(v))
        }
        "username" => {
            let v = val.as_str().to_string();
            app.settings.username = v.clone();
            Ok(Llsd::String(v))
        }
        "autohoverheight" | "auto_hover_height" => {
            let v = val.as_bool();
            app.settings.auto_hover_height = v;
            Ok(Llsd::Boolean(v))
        }
        "occlusionculling" | "occlusion_culling" => {
            let v = val.as_bool();
            app.settings.occlusion_culling = v;
            Ok(Llsd::Boolean(v))
        }
        "texturebudgetmb" | "texture_budget_mb" => {
            let v = val.as_u32();
            app.settings.texture_budget_mb = v;
            Ok(Llsd::Integer(v as i32))
        }
        "lodfactor" | "lod_factor" => {
            let v = val.as_f32().clamp(0.1, 10.0);
            app.settings.lod_factor = v;
            Ok(Llsd::Real(v as f64))
        }
        "uiscale" | "ui_scale" => {
            let v = val.as_f32().clamp(0.5, 3.0);
            app.settings.ui_scale = v;
            Ok(Llsd::Real(v as f64))
        }
        "glow" => {
            let v = val.as_bool();
            app.settings.glow = v;
            Ok(Llsd::Boolean(v))
        }
        "bloom" => {
            let v = val.as_bool();
            app.settings.bloom = v;
            Ok(Llsd::Boolean(v))
        }
        "sharpen" => {
            let v = val.as_f32().clamp(0.0, 1.0);
            app.settings.sharpen = v;
            Ok(Llsd::Real(v as f64))
        }
        "exposure" => {
            let v = val.as_f32().clamp(-4.0, 4.0);
            app.settings.exposure = v;
            Ok(Llsd::Real(v as f64))
        }
        other => Err(format!("Unknown setting variable '{other}'")),
    }
}

fn toggle_setting(app: &mut App, key: &str) -> Result<Llsd, String> {
    match key.to_ascii_lowercase().as_str() {
        "vsync" => {
            app.settings.vsync = !app.settings.vsync;
            Ok(Llsd::Boolean(app.settings.vsync))
        }
        "shadows" => {
            app.settings.shadows = !app.settings.shadows;
            Ok(Llsd::Boolean(app.settings.shadows))
        }
        "showhuds" | "show_huds" => {
            app.settings.show_huds = !app.settings.show_huds;
            Ok(Llsd::Boolean(app.settings.show_huds))
        }
        "sunbeacon" | "sun_beacon" => {
            app.settings.sun_beacon = !app.settings.sun_beacon;
            Ok(Llsd::Boolean(app.settings.sun_beacon))
        }
        "moonbeacon" | "moon_beacon" => {
            app.settings.moon_beacon = !app.settings.moon_beacon;
            Ok(Llsd::Boolean(app.settings.moon_beacon))
        }
        "autohoverheight" | "auto_hover_height" => {
            app.settings.auto_hover_height = !app.settings.auto_hover_height;
            Ok(Llsd::Boolean(app.settings.auto_hover_height))
        }
        "occlusionculling" | "occlusion_culling" => {
            app.settings.occlusion_culling = !app.settings.occlusion_culling;
            Ok(Llsd::Boolean(app.settings.occlusion_culling))
        }
        "glow" => {
            app.settings.glow = !app.settings.glow;
            Ok(Llsd::Boolean(app.settings.glow))
        }
        "bloom" => {
            app.settings.bloom = !app.settings.bloom;
            Ok(Llsd::Boolean(app.settings.bloom))
        }
        other => Err(format!("Setting '{other}' is not a toggleable boolean variable")),
    }
}

// ---------------------------------------------------------------------------
// Reply Dispatch & Event Broadcasting
// ---------------------------------------------------------------------------

fn send_reply(app: &App, reply_pump: &str, mut reply: Llsd, request: &Llsd) {
    let pump_name = if !reply_pump.is_empty() {
        reply_pump
    } else if let Some(leap) = &app.leap {
        leap.reply_pump()
    } else {
        return;
    };
    stamp_reqid(&mut reply, request);
    if let Some(leap) = &app.leap {
        let pump = leap.pumps().obtain(pump_name);
        let has_listeners = !pump.listener_names().is_empty();
        pump.post(&reply);
        if !has_listeners {
            let frame = aurora_leap::LeapEncoder::encode_message(pump_name, &reply);
            let _ = leap.outbound_tx().send(frame);
        }
    }
}

/// Forward internal viewer network events to LEAP event pumps.
pub fn broadcast_net_event(leap: &LeapManager, ev: &NetEvent) {
    match ev {
        NetEvent::Chat(msg) => {
            broadcast_chat(leap, msg);
        }
        NetEvent::InstantMessage(im) => {
            broadcast_im(leap, im);
        }
        NetEvent::LoggedIn(resp) => {
            let data = llsd_map! {
                "event" => "LoggedIn",
                "agent_id" => resp.agent_id.to_string(),
                "session_id" => resp.session_id.to_string(),
                "first_name" => resp.first_name.clone(),
                "last_name" => resp.last_name.clone(),
            };
            leap.post("LLLogin", data.clone());
            leap.post("login", data);
            broadcast_startup_state(leap, "STATE_STARTED", "Login completed");
        }
        NetEvent::LoginProgress { message, fraction } => {
            let data = llsd_map! {
                "event" => "LoginProgress",
                "message" => message.clone(),
                "fraction" => *fraction as f64,
            };
            leap.post("LLLogin", data.clone());
            leap.post("login", data);
        }
        NetEvent::LoginFailed { error, .. } => {
            let data = llsd_map! {
                "event" => "LoginFailed",
                "error" => error.clone(),
            };
            leap.post("LLLogin", data.clone());
            leap.post("login", data);
            broadcast_startup_state(leap, "STATE_LOGIN_FAILED", error);
        }
        NetEvent::TeleportFinished { handle, flying } => {
            let data = llsd_map! {
                "event" => "TeleportFinished",
                "region_handle" => Llsd::Real(*handle as f64),
                "flying" => *flying,
            };
            leap.post("teleport", data.clone());
            leap.post("LLTeleport", data);
        }
        NetEvent::TeleportStarted => {
            let data = llsd_map! {
                "event" => "TeleportStarted",
            };
            leap.post("teleport", data.clone());
            leap.post("LLTeleport", data);
        }
        NetEvent::TeleportProgress { message } => {
            let data = llsd_map! {
                "event" => "TeleportProgress",
                "message" => message.clone(),
            };
            leap.post("teleport", data.clone());
            leap.post("LLTeleport", data);
        }
        _ => {}
    }
}

pub fn broadcast_chat(leap: &LeapManager, msg: &aurora_net::ChatMessage) {
    let payload = llsd_map! {
        "from" => msg.from_name.clone(),
        "from_id" => msg.source_id.to_string(),
        "owner_id" => msg.owner_id.to_string(),
        "message" => msg.message.clone(),
        "text" => msg.message.clone(),
        "type" => msg.chat_type.to_u8() as i32,
        "channel" => 0,
        "position" => Llsd::Array(vec![
            Llsd::Real(msg.position.x as f64),
            Llsd::Real(msg.position.y as f64),
            Llsd::Real(msg.position.z as f64),
        ]),
    };
    leap.post("chat", payload.clone());
    leap.post("LLChat", payload);
}

pub fn broadcast_im(leap: &LeapManager, im: &aurora_net::InstantMessage) {
    let payload = llsd_map! {
        "from_id" => im.from_agent_id.to_string(),
        "from_name" => im.from_name.clone(),
        "to_id" => im.to_agent_id.to_string(),
        "dialog" => im.dialog as i32,
        "session_id" => im.session_id.to_string(),
        "message" => im.message.clone(),
        "timestamp" => im.timestamp,
    };
    leap.post("im", payload.clone());
    leap.post("LLInstantMessage", payload);
}

pub fn broadcast_startup_state(leap: &LeapManager, state: &str, desc: &str) {
    let payload = llsd_map! {
        "state" => state.to_string(),
        "desc" => desc.to_string(),
    };
    leap.post("LLStartUp", payload.clone());
    leap.post("StartupState", payload);
}
