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
