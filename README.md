# Harbor Python

Async Python client for connecting locally to Harbor Sleep Cameras.

`harbor-python` speaks directly to Harbor devices on your local network over MQTT, using the camera certificate material issued for your setup. It provides typed event parsing, device state tracking, command publishing, and helpers for configuring local WHIP streaming targets.

## Installation

```bash
pip install harbor-python
```

Python 3.11 or newer is required.

## Quick Start

```python
import asyncio

from harbor import Harbor, HarborCamera, HarborCameraConfig, HeartbeatUpdate


async def main() -> None:
    config = HarborCameraConfig(
        serial="CAMERA_SERIAL",
        ip_address="192.168.1.50",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
    )

    harbor = Harbor()
    camera = HarborCamera(config)

    camera.subscribe_updates(
        lambda state: print(f"{state.serial} values: {state.values}")
    )
    camera.subscribe(
        HeartbeatUpdate,
        lambda event: print(f"temperature: {event.payload.temperature}"),
    )

    harbor.add_device(camera)
    harbor.add_camera_connection(config)

    try:
        await harbor.start()
        await asyncio.Event().wait()
    finally:
        await harbor.stop()


asyncio.run(main())
```

On Windows, `aiomqtt` works best with the selector event loop policy:

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

## Certificate Configuration

`HarborCameraConfig` accepts certificate material in either form:

```python
HarborCameraConfig(
    serial="CAMERA_SERIAL",
    ip_address="192.168.1.50",
    cert_pem="<certificate PEM contents>",
    key_pem="<private key PEM contents>",
)
```

or:

```python
HarborCameraConfig(
    serial="CAMERA_SERIAL",
    ip_address="192.168.1.50",
    cert_path="/path/to/cert.pem",
    key_path="/path/to/key.pem",
    cert_dir="/path/to/ca-directory",
)
```

When both PEM strings and file paths are provided, the in-memory PEM values are used.

## Commands

Camera commands can be published directly:

```python
await harbor.publish_camera_command("CAMERA_SERIAL", "some-command", {"value": True})
```

For request/response commands, use `request_camera_command` or the settings helper:

```python
settings = await harbor.get_camera_settings("CAMERA_SERIAL")
print(settings.settings)
```

### Camera controls

```python
await harbor.set_camera_on("CAMERA_SERIAL", False)      # privacy: pause the stream
await harbor.set_night_mode("CAMERA_SERIAL", "auto")    # "auto" | "on" | "off"
await harbor.set_video_flip("CAMERA_SERIAL", True)      # rotate the image 180°
await harbor.set_clock_display("CAMERA_SERIAL", False)  # clock overlay on the video
await harbor.set_temperature_scale("CAMERA_SERIAL", "C")  # "F" | "C"
await harbor.update_camera_settings(
    "CAMERA_SERIAL", {"preference_video_ir_brightness": 40}
)
```

Each control writes one preference and refreshes device state, so
`camera.state.values` reflects the change once the call returns:

| `camera.state.values[...]` | Type | Setting |
|---------------------------|------|---------|
| `camera_on` | `bool` | `preference_stream_paused` (inverted) |
| `night_mode_preference` | `"auto"` \| `"on"` \| `"off"` | `preference_video_night_mode` |
| `night_mode` | `bool` | runtime IR state (read-only, see below) |
| `video_flip` | `bool` | `preference_video_flip` |
| `clock_display` | `bool` | `preference_video_has_clock_display` |
| `temperature_scale` | `"F"` \| `"C"` | `preference_temperature_scale` |

`set_video_flip` and `set_clock_display` take real booleans — `1`/`0` and
`"true"` raise `ValueError` rather than being sent as a number or string,
which the firmware would reject.

The enum setters validate against the firmware's own option list, exported as
`NIGHT_MODE_MODES` and `TEMPERATURE_SCALES`. Matching is exact: `"f"` raises
`ValueError`, because the device compares the string verbatim. State values
preserve case for the same reason — what you read back is always something you
can write.

#### Night mode

Night mode is a **three-way preference**, not a boolean — `"auto"`, `"on"` or
`"off"` (default `"auto"`). Passing a bool raises `ValueError` rather than
guessing at a mode. A camera exposes two separate night-mode values:

| `camera.state.values[...]` | Type | Meaning |
|---------------------------|------|---------|
| `night_mode_preference` | `"auto"` \| `"on"` \| `"off"` | The setting. This is what `set_night_mode` writes and what reads back. |
| `night_mode` | `bool` | Whether IR is engaged *right now*. Read-only and device-driven — under `"auto"` it flips on its own as light levels change. |

Consumers building a UI entity should bind it to `night_mode_preference`, since
`night_mode` moves independently of any command.

### Command errors

A rejected command raises `HarborCommandError`, which carries the parsed
`status` and the firmware's per-field `errors` list:

```python
from harbor import HarborCommandError, HarborUnsupportedCommandError

try:
    await harbor.set_night_mode("CAMERA_SERIAL", "on")
except HarborUnsupportedCommandError:
    ...  # firmware has no such command; permanent, so stop offering the feature
except HarborCommandError as err:
    print(err.status, err.errors)  # e.g. "REQUEST_MALFORMED", [{"error_code": "INVALID_VALUE", ...}]
```

`HarborUnsupportedCommandError` is a subclass of `HarborCommandError`, raised
only on a `RESOURCE_NOT_FOUND` status. That means the firmware has no handler
for the command at all, so retrying can never succeed.

### Firmware compatibility

Commands are verified against real hardware, most recently a camera running
`os_version` **2.8.0** / `app_version` **2.8.0-rc1+c1b0a32**: `ping`,
`get-settings`, `update-settings`, `pause-stream`, `unpause-stream`,
`set-night-mode-ir-brightness`, `update-operating-mode`, `set-scheduled-reboot`
and `list-viewers` all respond `OK`. See `mqtt_home_assistant.md` for the full
audit and payload shapes.

## WHIP Endpoint

Harbor cameras allow custom WHIP endpoints. This tells the camera where to stream and works with tools that support WHIP, including go2rtc and Frigate.

### Setting up WHIP Ingestion (go2rtc)

You can self-host go2rtc in many ways; see the [go2rtc installation guide](https://go2rtc.org/#installation). If you use Home Assistant, the easiest option is the [go2rtc add-on](https://go2rtc.org/#go2rtc-home-assistant-add-on).

Once go2rtc is running, add a stream keyed by your camera serial number:

```yaml
api:
  listen: ":1984" # Change this if you use a non-default port.

streams:
  "CAMERA_SERIAL":
```

### Setting up WHIP Ingestion (Frigate)

Frigate runs an instance of go2rtc under the hood. Add the following to your Frigate config:

```yaml
go2rtc:
  api:
    listen: ":1984"
  streams:
    "CAMERA_SERIAL":
```

### Setting the Endpoint

1. Open your Harbor app
2. Go to Live
3. Open Camera Settings
4. Scroll down and click on Advanced Settings
5. Enter the WHIP endpoint, for example: `http://192.168.1.10:1984/api/webrtc?dst=CAMERA_SERIAL`

Replace `CAMERA_SERIAL` with your camera serial number and `192.168.1.10` with the IP address of your go2rtc or Frigate server.

## Development

```bash
uv sync
uv run pytest
```

## License

Licensed under the Apache License 2.0.
