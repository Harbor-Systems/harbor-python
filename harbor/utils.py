import logging
import ssl

from .config import HarborCameraConfig

_LOGGER = logging.getLogger(__name__)


def get_camera_host(camera_config: HarborCameraConfig) -> str:
    if camera_config.ip_address:
        return camera_config.ip_address
    return f"harborc-{camera_config.serial}.local"


def build_ssl_context(camera_config: HarborCameraConfig) -> ssl.SSLContext:
    _LOGGER.info(
        "Harbor: Building SSL context with cert_path=%s, key_path=%s", camera_config.cert_path, camera_config.key_path
    )
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(certfile=camera_config.cert_path, keyfile=camera_config.key_path)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def get_ssl_context(camera_config: HarborCameraConfig, cache: dict | None = None) -> ssl.SSLContext:
    if cache is None:
        _LOGGER.info("Harbor: Creating new SSL context (no cache)")
        return build_ssl_context(camera_config)

    key = camera_config.serial
    if key in cache:
        _LOGGER.debug("Harbor: Returning cached SSL context for camera %s", camera_config.serial)
        return cache[key]

    _LOGGER.info("Harbor: Creating new SSL context for camera %s", camera_config.serial)
    ctx = build_ssl_context(camera_config)
    cache[key] = ctx
    return ctx
