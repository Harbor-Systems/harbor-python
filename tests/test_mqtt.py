import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from harbor.config import HarborCameraConfig
from harbor.mqtt import HarborMQTTClient


def test_client_creation():
    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
        cert_dir="/path/to/cert_dir",
        ip_address="192.168.1.100",
    )

    client = HarborMQTTClient(config)
    assert client.camera_config.serial == "TEST123"
    assert client.camera_config.ip_address == "192.168.1.100"
    print("Client creation test passed!")


def test_message_callback():
    messages = []

    def callback(serial: str, topic: str, payload):
        messages.append((serial, topic, payload))

    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
        cert_dir="/path/to/cert_dir",
    )

    client = HarborMQTTClient(config, message_callback=callback)
    client._handle_message("test/topic", '{"test": "data"}')

    assert len(messages) == 1
    assert messages[0][0] == "TEST123"
    assert messages[0][1] == "test/topic"
    assert messages[0][2] == {"test": "data"}
    print("Message callback test passed!")


if __name__ == "__main__":
    test_client_creation()
    test_message_callback()
    print("\nAll tests passed!")
