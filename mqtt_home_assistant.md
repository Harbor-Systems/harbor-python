# Harbor MQTT — Home Assistant Integration Guide

This document describes the **safe subset** of Harbor's MQTT interface intended for a
Home Assistant (HA) integration. It covers the telemetry a camera publishes (for HA
*sensors*) and the commands it is safe to send (for HA *switches / buttons*).

> ⚠️ **Scope note for whoever builds the HA integration:** Harbor exposes many more
> MQTT commands than are listed here. The ones omitted change WiFi/access-point
> credentials, firmware, boot partitions, remote-access tunnels, run shell commands,
> etc. **Do not add HA entities for anything not in this document.** A short
> deny-list is included at the end so you know what to avoid, but none of those should
> be wired into HA. If you think you need one, ask the Harbor team first.

---

## 1. Connection

Home Assistant connects to the camera's on-device NanoMQ broker on your LAN over
**mutual TLS**. The broker exposes two TLS listeners; **HA must use the external
listener** — it has its own certificate chain intended for third-party clients:

| Listener | Bind | Cert chain | Use |
|----------|------|-----------|-----|
| Internal | `:8883` | camera cert, `root_ca.pem` CA | Harbor's own on-device services — **do not use** |
| **External** | **`:8884`** | external server cert + external CA | **This is the one for Home Assistant** |

**How to connect (external listener):**
- **Host:** the camera on your LAN, **port `8884`**.
- **Protocol:** MQTT v5, TLS required.
- **Client certificate:** HA must present a client cert **signed by the external CA**
  (the cert chain behind `/mnt/settings/ssl/external/...` on the device), *not* the
  internal `camera.crt`. The broker sets `verify_peer = true` and
  `fail_if_no_peer_cert = true`, so a client cert is mandatory — a connection without a
  valid external cert is dropped.
- **Username/password:** none. The broker allows anonymous auth; the client
  **certificate** is what authenticates HA.

Other broker facts:
- **QoS:** `0` by default (configurable via the `MQTT_QOS` env var on the device).
- **Retain:** Harbor never publishes retained messages. HA must be subscribed *before*
  an event fires to see it. Do not expect to read the "last state" from a retained topic.
- **Client ID:** pick a stable one for HA (e.g. `home-assistant`).

> Note: the camera also mirrors this traffic to a separate Harbor cloud broker, but that
> is internal to Harbor — the HA integration neither connects to it nor needs it.

Source: broker config `meta-harbor` →
`recipes-connectivity/nanomq/files/nanomq.conf` (listeners `ssl` :8883 and
`ssl.external` :8884); app side `src/messaging_protocols/mqtt_pubsub.cc`.

---

## 2. Topic structure

Every topic is namespaced under the camera's resource id, which is:

```
cameras/<CAMERA_ID>
```

`<CAMERA_ID>` is the per-device id (env `CAMERA_ID`). From that root:

| Purpose | Topic pattern | Direction |
|---------|---------------|-----------|
| Command (HA → camera) | `cameras/<CAMERA_ID>/<command>` | HA publishes |
| Command response | `cameras/<CAMERA_ID>/responses/<command>` | Camera publishes reply |
| Event / telemetry | `cameras/<CAMERA_ID>/events/<event>` | Camera publishes |
| Panic event | `cameras/<CAMERA_ID>/events/panic/<name>` | Camera publishes |

The camera subscribes to `cameras/<CAMERA_ID>/#` and, for any command it handles,
publishes the handler's result to `cameras/<CAMERA_ID>/responses/<command>`.

**Request/response pattern:** send a JSON payload to the command topic; if you include a
`seq` field it is echoed back on the matching `responses/...` topic. Responses generally
carry a `status` string (`"OK"`, `"REQUEST_MALFORMED"`, etc.) and, on error, an `error`
message.

Source: `src/application.cc:486` (subscribe), `:566-604` (routing + response),
`harbor-common/include/harbor-common/utils/topics.hh`.

---

## 3. Telemetry to subscribe to (HA sensors)

Subscribe to these `events/...` topics. All are safe to consume — they are read-only
signals the camera already emits.

### `cameras/<CAMERA_ID>/events/heartbeat`
Periodic device health. Good for temperature sensors and an "online" heartbeat.
```json
{
  "temperature": 42,
  "raw_temperature": 42.3,
  "sensor_temperature": 41.8,
  "ntc_temperature": 40.1,
  "ntc_adc_voltage": 0.83,
  "image_sensor_temperature": 45.0,
  "efuse_voltage": 0.91,
  "os_version": "1.2.3",
  "app_version": "2.7.1"
}
```
Source: `src/application.cc:410`.

### `cameras/<CAMERA_ID>/events/local-livekit-heartbeat`
Streaming/network heartbeat (only sent while the stream is active and not updating).
Contains media-capture statistics plus:
```json
{
  "app_version": "2.7.1",
  "os_version": "1.2.3",
  "network_bars": 4
}
```
Use `network_bars` for a WiFi-signal sensor. Source: `src/application.cc:461`.

### `cameras/<CAMERA_ID>/events/up`
Published when the camera comes online / (re)subscribes. Useful as an availability
signal and to read current versions and state.
```json
{
  "os_version": "1.2.3",
  "app_version": "2.7.1",
  "release_version": "...",
  "golden_image_crc32": "...",
  "local_ip_address": "192.168.1.50",
  "settings": { "...full settings object..." },
  "state":    { "...video/stream state..." },
  "reboot_type": "...", "reboot_reason": "...", "reboot_timestamp": 0
}
```
> Note: the `settings` object here is the full device configuration. It's fine to read,
> but treat it as informational — surface only the fields you actually need in HA.
Source: `src/application.cc:515-565`.

### `cameras/<CAMERA_ID>/events/down`
Last-will + graceful-shutdown message; best signal for an "offline" binary sensor.
```json
{ "reason": "unexpected_disconnect", "app_version": "...", "os_version": "...", "release_version": "..." }
```
Source: `src/messaging_protocols/mqtt_pubsub.cc:42-43,136,207`.

### `cameras/<CAMERA_ID>/events/settings`
Emitted whenever settings change. Mirror of the settings/state you'd get from
`get-settings`. Source: `src/application.cc:729`.

### `cameras/<CAMERA_ID>/events/sound-anomaly-detected`
Fires when the camera detects a sound anomaly (e.g. crying). Great for HA automations /
event entities. Payload is produced by the media pipeline (anomaly metadata).
Source: `src/messaging_protocols/health_monitor_server.cc:149`.

### `cameras/<CAMERA_ID>/events/motion-detected`
Fires on motion detection. Same shape/usage as the sound anomaly above.
Source: `src/messaging_protocols/health_monitor_server.cc:165`.

### `cameras/<CAMERA_ID>/events/operating-mode-changed`
Emitted when the operating mode changes.

### `cameras/<CAMERA_ID>/events/stream-status-updated`
Emitted when the live-stream status changes (started/stopped/paused).

### `cameras/<CAMERA_ID>/events/sleep-insights`
Emitted when a sleep-insights inference completes.

### `cameras/<CAMERA_ID>/events/update-event`
Software-update progress.
```json
{ "state": "...", "progress": 0, "reason": "..." }
```
Source: `src/update_manager.cc` (UPDATE_EVENT).

> Event names above map to the `EVENT(...)` macros in
> `harbor-common/include/harbor-common/utils/topics.hh` (lines 22-48).

---

## 4. Commands that are safe to send (HA switches / buttons)

Publish JSON to `cameras/<CAMERA_ID>/<command>`; read the reply on
`cameras/<CAMERA_ID>/responses/<command>`. Include a `seq` to correlate the response.

### `ping`
Health check. Empty payload `{}`. Response: `{ "status": "OK" }`.
Source: `handlers.cc:830`.

### `get-settings`
Read current settings + state. Empty payload `{}`.
```json
{
  "settings": { "..." },
  "state": { "..." },
  "is_updating": false
}
```
Source: `handlers.cc:94`.

### `pause-stream` / `unpause-stream`
Privacy toggle — turns the camera stream off / on. This is the natural HA "camera
on/off" switch.
```json
{ "viewer_id": "home-assistant" }
```
`unpause-stream` returns an error if an update is in progress. Sources:
`handlers.cc:768` / `handlers.cc:791`.

### `update-settings`
Write camera preferences. This is the command the official app uses for every
settings change — there is no per-setting command.
```json
{
  "seq": "<uuid>",
  "settings": { "preference_video_night_mode": "auto" },
  "client": "home-assistant",
  "triggeredBy": "users/<user-id>"
}
```
`settings` carries only the keys being changed; the camera merges them and
echoes back the applied subset alongside `"status": "OK"`. On a bad value it
returns `status` `REQUEST_MALFORMED` plus an `errors` array that includes the
accepted schema:
```json
{ "errors": [ { "error_code": "INVALID_VALUE",
                "key": "/preference_video_night_mode",
                "schema": { "default": "auto", "options": ["auto","on","off"], "type": "string" },
                "value": "bogus-mode" } ],
  "status": "REQUEST_MALFORMED" }
```

#### Boolean preferences (HA switches)
These two are genuine booleans and map directly onto switches:

| Key | Default | Meaning |
|-----|---------|---------|
| `preference_video_flip` | `false` | Rotate the image 180° |
| `preference_video_has_clock_display` | `true` | Clock overlay burned into the video |

```json
{ "settings": { "preference_video_flip": true } }
```

Send JSON booleans, not `1`/`0` — the firmware types these as `boolean` and
rejects a number.

#### Enumerated preferences (HA selects)
Every enum below is validated by the firmware against a fixed option list, and
matched **verbatim** — `"f"` is rejected where `"F"` is accepted.

| Key | Options | Default | Notes |
|-----|---------|---------|-------|
| `preference_video_night_mode` | `auto`, `on`, `off` | `auto` | See below |
| `preference_temperature_scale` | `F`, `C` | `F` | Safe |
| `log_level` | `trace`, `verbose`, `debug`, `info`, `warning`, `error`, `fatal` | `info` | Diagnostic — not worth an HA entity |
| `preference_operating_mode` | `global`, `direct` | `global` | ⚠️ `direct` pairs the camera with a monitor off the home LAN — can drop it off the network |
| `preference_connection_band` | `auto`, `a`, `bg` | `auto` | ⚠️ WiFi radio config — see the deny-list |
| `preference_stream_config.resolution` | `2688x1520`, `2432x1520`, `1920x1080`, `1728x1080`, `1280x720`, `1152x720` | `1728x1080` | Nested; restarts the stream |
| `preference_anomaly_configs.active_config` | `care`, `primary` | `primary` | Nested; swaps the whole detection profile |
| `preference_anomaly_configs.*.motion[].motion_level` | `low`, `medium`, `high` | — | Nested inside an array |

#### Night mode
Night mode is set through `update-settings`, **not** a dedicated command:

```json
{ "settings": { "preference_video_night_mode": "on" } }
```

> ⚠️ There is no `update-night-mode` command. Publishing to it returns
> `RESOURCE_NOT_FOUND` on firmware 2.8.0, and the string appears nowhere in the
> official app — the app sends `update-settings`.

**Night mode is a three-way preference, not a boolean.** Accepted values are
`"auto"`, `"on"` and `"off"`, default `"auto"`. In Home Assistant this must be
a **select**, not a switch.

Do not confuse the two night-mode fields in a settings payload:

| Field | Where | Type | Meaning |
|-------|-------|------|---------|
| `preference_video_night_mode` | `settings` | `"auto"`\|`"on"`\|`"off"` | The **setting**. Writable; this is what you read back after a write. |
| `video_night_mode` | `state` | bool | Whether IR is engaged **right now**. Read-only, device-driven — under `auto` it flips by itself as light changes. |

Under `"auto"` the preference stays `"auto"` while the runtime bool moves on its
own, so an entity whose state is derived from the runtime bool will not match
the command that set it.

### `set-night-mode-ir-brightness`
Set IR LED brightness for night mode. Value is range-checked on the device.
```json
{ "ir_brightness": 50 }
```
Response: `{ "message": "Night mode IR brightness updated successfully" }` or an
`error`. Source: `handlers.cc:1258`.

### `update-operating-mode`
Change the camera's operating mode.
```json
{ "operating_mode": "..." }
```
Source: `handlers.cc:818`.

### `set-scheduled-reboot`
Enable/disable a daily scheduled reboot.
```json
{ "enabled": true, "reboot_time": "03:30" }
```
`reboot_time` must be `hh:mm` (24h) and is validated. Send `{ "enabled": false }` to
disable. Source: `handlers.cc:1048`.

### Moments (recorded clips)
- `list-moments` — list saved clips.
- `save-moment` — save a clip for a time range: `{ "start": <t>, "end": <t> }`.
- `list-viewers` — list current stream viewers.
- `run-sleep-insights` — trigger a sleep-insights run. Empty payload.

Sources: `handlers.cc` (`save_moment:131`, and the handler map at `handlers.cc:405-545`).

> These are the user-facing "public" commands. The full command constants are in
> `harbor-common/include/harbor-common/utils/topics.hh` lines 56-111.

---

## 5. Do NOT expose these (sensitive — for awareness only)

The camera also handles the commands below. **None of these should be wired into a
Home Assistant entity.** They can change credentials, network config, firmware, or run
privileged operations, and would expose the device if triggered from HA. Listed so the
integration author knows to steer clear.

**WiFi / access point / network credentials**
`connect-to-network`, `create-ap`, `start-ap`, `get-secure-ap-info`,
`get-onboarding-info`, `update-interfaces`, `get-wifi-band-settings`,
`set-wifi-band-settings`, `list-networks`, `list-scanned-networks`, `remove-network`,
`pin-bssid`, `unpin-all-bssids`, `rescan-networks`, `get-roaming-settings`,
`set-roaming-settings`, `set-auto-pinning-settings`.

**Firmware / boot / destructive**
`update-now`, `swap-partitions`, `internal/reset` (factory reset),
`internal/software-restart`, `internal/hardware-restart`,
`remove-certificate-not-in-secure-storage-flag`.

**Remote access / privileged execution**
`start-frpc`, `stop-frpc` (reverse tunnel), `restart-service` (runs `systemctl`),
`ap-isolation-test` (runs a network probe).

> The `internal/...` commands live under `cameras/<CAMERA_ID>/internal/...` and are meant
> for the monitor/service tooling, not end users. Source:
> `harbor-common/include/harbor-common/utils/topics.hh:113-131`.
>
> Separately, the on-device HTTP server (not MQTT) has a `/client-certs` route that
> returns the device's client certificate and key — never proxy or expose that.

---

## 6. Quick reference

| I want to… | Topic | Payload |
|------------|-------|---------|
| Know the temperature | sub `events/heartbeat` | — |
| Know WiFi signal | sub `events/local-livekit-heartbeat` | — |
| Know online/offline | sub `events/up` / `events/down` | — |
| Get motion events | sub `events/motion-detected` | — |
| Get sound/cry events | sub `events/sound-anomaly-detected` | — |
| Turn camera off/on | pub `pause-stream` / `unpause-stream` | `{"viewer_id":"home-assistant"}` |
| Set night mode | pub `update-settings` | `{"settings":{"preference_video_night_mode":"auto"}}` |
| Set IR brightness | pub `set-night-mode-ir-brightness` | `{"ir_brightness":50}` |
| Change any preference | pub `update-settings` | `{"settings":{"<key>":<value>}}` |
| Read all settings | pub `get-settings` | `{}` |
| Health check | pub `ping` | `{}` |
| Schedule nightly reboot | pub `set-scheduled-reboot` | `{"enabled":true,"reboot_time":"03:30"}` |

All topics are prefixed with `cameras/<CAMERA_ID>/`.

---

## 7. Verified against firmware

Every command below was published to a real camera (serial `2409001608`,
`os_version` **2.8.0**, `app_version` **2.8.0-rc1+c1b0a32**). Writes echoed the
value the camera already held, so each probe was a no-op.

| Command | Result |
|---------|--------|
| `ping` | ✅ `OK` |
| `get-settings` | ✅ `OK` |
| `update-settings` | ✅ `OK` — echoes the applied subset |
| `set-night-mode-ir-brightness` | ✅ `OK` — `"Night mode IR brightness updated successfully"` |
| `update-operating-mode` | ✅ `OK` |
| `set-scheduled-reboot` | ✅ `OK` |
| `list-viewers` | ✅ `OK` |
| `pause-stream` / `unpause-stream` | ✅ known working (not re-probed — would interrupt the stream) |
| `update-night-mode` | ❌ **`RESOURCE_NOT_FOUND` — no such command** |
| `list-moments` | ⚠️ no response within 8s; may not be implemented on this build |

A `RESOURCE_NOT_FOUND` status means the firmware has no handler for that
command. It is permanent for the build, not transient, so a client should stop
offering the feature rather than retry.
