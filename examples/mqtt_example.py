import asyncio
import logging
from pathlib import Path

from harbor import HarborCamera, HarborCameraConfig, HarborMonitor
from harbor.core import Harbor

logging.basicConfig(level=logging.INFO)
logging.getLogger("harbor").setLevel(logging.DEBUG)


async def main():
    # Resolve keys directory relative to this script's location
    base_path = Path(__file__).resolve().parent.parent
    keys_dir = base_path / "keys"

    config = HarborCameraConfig(
        serial="2409001608",
        cert_path=str(keys_dir / "cert.pem"),
        key_path=str(keys_dir / "key.pem"),
        cert_dir=str(keys_dir),
        ip_address="192.168.1.208",
    )

    # High-level Harbor instance
    harbor = Harbor()

    # Create devices
    camera = HarborCamera(config)  # Uses config.serial as ID
    monitor = HarborMonitor("MONITOR_001")  # Arbitrary ID

    # Add devices to Harbor
    harbor.add_device(camera)
    harbor.add_device(monitor)

    # Add connection
    harbor.add_camera_connection(config)

    try:
        await harbor.start()
        print("Harbor started. Press Ctrl+C to stop...")

        # Monitor Loop
        while True:
            await asyncio.sleep(5)

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await harbor.stop()


if __name__ == "__main__":
    import sys

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
