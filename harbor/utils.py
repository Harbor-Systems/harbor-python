import hashlib
import logging
import os
import ssl
import tempfile

from .config import HarborCameraConfig

_LOGGER = logging.getLogger(__name__)


def get_camera_host(camera_config: HarborCameraConfig) -> str:
    if camera_config.ip_address:
        return camera_config.ip_address
    return f"harborc-{camera_config.serial}.local"


def get_ssl_cache_key(camera_config: HarborCameraConfig) -> str:
    """Return the cache key for the SSL context built from this config.

    Keyed off the certificate material itself so a config carrying new
    credentials never reuses a stale context.
    """
    if camera_config.cert_pem is not None and camera_config.key_pem is not None:
        digest = hashlib.sha256()
        digest.update(camera_config.cert_pem.encode())
        digest.update(b"\x00")
        digest.update(camera_config.key_pem.encode())
        return f"pem:{digest.hexdigest()}"
    return f"path:{camera_config.cert_path}:{camera_config.key_path}"


def _write_private_file(path: str, data: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    # PEM must stay byte-exact: no platform newline translation or locale encoding.
    with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(data)


def build_ssl_context(camera_config: HarborCameraConfig) -> ssl.SSLContext:
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

    if camera_config.cert_pem is not None and camera_config.key_pem is not None:
        _LOGGER.info("Harbor: Building SSL context from in-memory PEM data for camera %s", camera_config.serial)
        # ssl.SSLContext.load_cert_chain only accepts file paths, so stage the
        # PEM data in a short-lived private temp dir that is removed before
        # this function returns.
        with tempfile.TemporaryDirectory(prefix="harbor-tls-") as tmp_dir:
            cert_file = os.path.join(tmp_dir, "cert.pem")
            key_file = os.path.join(tmp_dir, "key.pem")
            _write_private_file(cert_file, camera_config.cert_pem)
            _write_private_file(key_file, camera_config.key_pem)
            ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
    else:
        _LOGGER.info(
            "Harbor: Building SSL context with cert_path=%s, key_path=%s",
            camera_config.cert_path,
            camera_config.key_path,
        )
        if camera_config.cert_path is None or camera_config.key_path is None:
            raise ValueError("HarborCameraConfig has no certificate material")
        ctx.load_cert_chain(certfile=camera_config.cert_path, keyfile=camera_config.key_path)

    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def get_ssl_context(camera_config: HarborCameraConfig, cache: dict | None = None) -> ssl.SSLContext:
    if cache is None:
        _LOGGER.info("Harbor: Creating new SSL context (no cache)")
        return build_ssl_context(camera_config)

    key = get_ssl_cache_key(camera_config)
    if key in cache:
        _LOGGER.debug("Harbor: Returning cached SSL context for camera %s", camera_config.serial)
        return cache[key]

    _LOGGER.info("Harbor: Creating new SSL context for camera %s", camera_config.serial)
    ctx = build_ssl_context(camera_config)
    cache[key] = ctx
    return ctx
