from dataclasses import dataclass


@dataclass(frozen=True)
class HarborCameraConfig:
    """Connection configuration for a Harbor camera.

    Client certificate material can be provided either as in-memory PEM
    strings (``cert_pem``/``key_pem``) or as file paths
    (``cert_path``/``key_path``). PEM strings are preferred for consumers
    that should not manage certificate files themselves: while building the
    SSL context the library briefly stages the PEM data in a private
    temporary directory (0600 files) that is deleted before the build
    returns, so the caller never handles files. When both are provided, the
    PEM strings win.
    """

    serial: str
    cert_path: str | None = None
    key_path: str | None = None
    cert_dir: str | None = None

    ip_address: str | None = None

    cert_pem: str | None = None
    key_pem: str | None = None

    def __post_init__(self) -> None:
        if (self.cert_pem is None) != (self.key_pem is None):
            raise ValueError("cert_pem and key_pem must be provided together")
        if (self.cert_path is None) != (self.key_path is None):
            raise ValueError("cert_path and key_path must be provided together")
        if self.cert_pem is None and self.cert_path is None:
            raise ValueError("Certificate material is required: provide cert_pem/key_pem or cert_path/key_path")
