from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HarborMQTTPayload(BaseModel):
    """Base model for Harbor MQTT payloads."""

    model_config = ConfigDict(extra="allow")


class LocalLivekitHeartbeatEvent(HarborMQTTPayload):
    """Payload for a local LiveKit heartbeat."""

    app_start_time: str | None = None
    app_version: str | None = None
    bitrate: float | None = None
    camera_present: bool | None = None
    camera_state: str | None = None
    is_healthy: dict[str, Any] = Field(default_factory=dict)
    network_bars: int | None = None
    os_version: str | None = None
    receiver_present: bool | None = None
    speaker_state: str | None = None
    stream_quality: str | None = None
    stream_start_time: str | None = None
    viewers_by_identity: dict[str, Any] = Field(default_factory=dict)
    viewers_by_identity_full: dict[str, Any] = Field(default_factory=dict)


class HeartbeatEvent(HarborMQTTPayload):
    """Payload for a device heartbeat."""

    app_version: str | None = None
    efuse_voltage: int | None = None
    image_sensor_temperature: float | None = None
    ntc_adc_voltage: int | None = None
    ntc_temperature: float | None = None
    os_version: str | None = None
    raw_temperature: float | None = None
    sensor_temperature: float | None = None
    temperature: float | None = None


class Settings(HarborMQTTPayload):
    """Camera settings included with a settings event."""

    log_level: str | None = None
    preference_ai: dict[str, Any] = Field(default_factory=dict)
    preference_anomaly_configs: dict[str, Any] = Field(default_factory=dict)
    preference_anomaly_throttle_duration_seconds: int | None = None
    preference_auto_pinning: bool | None = None
    preference_connection_band: str | None = None
    preference_connection_bssid: str | None = None
    preference_display_name: str | None = None
    preference_moment_length: int | None = None
    preference_operating_mode: str | None = None
    preference_scheduled_reboot: str | None = None
    preference_silence_alerting_until: str | None = None
    preference_stream_paused: bool | None = None
    preference_video_clock_display_tz_abbrev: str | None = None
    preference_video_clock_display_tz_offset: int | None = None
    preference_video_flip: bool | None = None
    preference_video_has_clock_display: bool | None = None
    preference_video_ir_brightness: int | None = None
    preference_video_night_mode: str | None = None


class SettingsState(HarborMQTTPayload):
    """Runtime state attached to a settings event."""

    application_state: int | None = None
    network_bars: int | None = None
    stream_state: int | None = None
    temperature: float | None = None
    video_night_mode: bool | None = None


class SettingsEvent(HarborMQTTPayload):
    """Payload for a settings event."""

    client: str | None = None
    is_updating: bool | None = None
    seq: str | None = None
    settings: Settings | None = None
    state: SettingsState | None = None
    triggered_by: str | None = None
    updated: dict[str, Any] = Field(default_factory=dict)


class ViewerJoinedEvent(HarborMQTTPayload):
    """Payload for a viewer joined event."""

    client: str | None = None
    identity: str | None = None
    is_local: bool | None = None
    role: str | None = None
    viewer_id: str | None = None


class ViewerLeftEvent(HarborMQTTPayload):
    """Payload for a viewer left event."""

    client: str | None = None
    identity: str | None = None
    is_local: bool | None = None
    role: str | None = None
    viewer_id: str | None = None


class MotionDetectedEvent(HarborMQTTPayload):
    """Payload for a motion detection event."""

    active_config: str | None = None
    duration: str | float | int | None = None
    filename: str | None = None
    level: str | None = None
    sensitivity: str | None = None
    threshold: str | None = None
    thumbnail: str | None = None
    timestamp: str | None = None
