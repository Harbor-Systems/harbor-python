from dataclasses import dataclass


@dataclass(frozen=True)
class HarborCameraConfig:
    serial: str
    cert_path: str
    key_path: str
    cert_dir: str

    ip_address: str | None = None
