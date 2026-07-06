from __future__ import annotations

import ssl

import pytest

from harbor.config import HarborCameraConfig
from harbor.utils import build_ssl_context, get_ssl_cache_key, get_ssl_context

from .data.certs import TEST_CERT_PEM, TEST_KEY_PEM


def test_config_accepts_paths() -> None:
    """Path-based configs should keep working unchanged."""

    config = HarborCameraConfig(
        serial="TEST123",
        cert_path="/path/to/cert.pem",
        key_path="/path/to/key.pem",
        cert_dir="/path/to/cert_dir",
    )

    assert config.cert_path == "/path/to/cert.pem"
    assert config.cert_pem is None


def test_config_accepts_pem_strings() -> None:
    """PEM material should be accepted without any file paths."""

    config = HarborCameraConfig(serial="TEST123", cert_pem=TEST_CERT_PEM, key_pem=TEST_KEY_PEM)

    assert config.cert_pem == TEST_CERT_PEM
    assert config.cert_path is None


def test_config_requires_some_certificate_material() -> None:
    with pytest.raises(ValueError):
        HarborCameraConfig(serial="TEST123")


def test_config_rejects_partial_pem() -> None:
    with pytest.raises(ValueError):
        HarborCameraConfig(serial="TEST123", cert_pem=TEST_CERT_PEM)


def test_config_rejects_partial_paths() -> None:
    with pytest.raises(ValueError):
        HarborCameraConfig(serial="TEST123", cert_path="/path/to/cert.pem")


def test_build_ssl_context_from_pem() -> None:
    """The SSL context should be built entirely from in-memory PEM data."""

    config = HarborCameraConfig(serial="TEST123", cert_pem=TEST_CERT_PEM, key_pem=TEST_KEY_PEM)

    ctx = build_ssl_context(config)

    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_NONE


def test_ssl_context_cache_keyed_off_pem_material() -> None:
    """The cache should hit on identical material and miss on different material."""

    cache: dict = {}
    config = HarborCameraConfig(serial="TEST123", cert_pem=TEST_CERT_PEM, key_pem=TEST_KEY_PEM)

    ctx_first = get_ssl_context(config, cache)
    ctx_second = get_ssl_context(config, cache)
    assert ctx_first is ctx_second
    assert len(cache) == 1

    same_material_other_serial = HarborCameraConfig(serial="OTHER", cert_pem=TEST_CERT_PEM, key_pem=TEST_KEY_PEM)
    assert get_ssl_cache_key(config) == get_ssl_cache_key(same_material_other_serial)

    different_material = HarborCameraConfig(serial="TEST123", cert_pem=TEST_CERT_PEM, key_pem=TEST_KEY_PEM + "\n")
    assert get_ssl_cache_key(config) != get_ssl_cache_key(different_material)


def test_ssl_cache_key_for_paths() -> None:
    pem_config = HarborCameraConfig(serial="TEST123", cert_pem=TEST_CERT_PEM, key_pem=TEST_KEY_PEM)
    path_config = HarborCameraConfig(serial="TEST123", cert_path="/a/cert.pem", key_path="/a/key.pem")

    assert get_ssl_cache_key(pem_config) != get_ssl_cache_key(path_config)
