from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class HarborMQTTPayload(BaseModel):
    """Base model for Harbor MQTT payloads."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)


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
    """Camera settings included with a settings event.

    These are the writable *preferences*. Send changes with the
    ``update-settings`` command; do not confuse them with the device-driven
    runtime values in :class:`SettingsState`.
    """

    log_level: str | None = None
    preference_ai: dict[str, Any] = Field(default_factory=dict)
    preference_anomaly_configs: dict[str, Any] = Field(default_factory=dict)
    preference_anomaly_throttle_duration_seconds: int | None = None
    preference_auto_pinning: bool | None = None
    preference_connection_band: str | None = None
    preference_connection_bssid: str | None = None
    preference_display_name: str | None = None
    preference_moment_length: int | None = None
    preference_no_alert_windows: list[Any] = Field(default_factory=list)
    preference_operating_mode: str | None = None
    preference_scheduled_reboot: str | None = None
    preference_silence_alerting_until: str | None = None
    preference_stream_config: dict[str, Any] = Field(default_factory=dict)
    preference_stream_paused: bool | None = None
    preference_temperature_calibration_offset: float | None = None
    preference_temperature_scale: str | None = None
    preference_video_brightness_low_light: int | None = None
    preference_video_clock_display_tz_abbrev: str | None = None
    preference_video_clock_display_tz_offset: int | None = None
    preference_video_flip: bool | None = None
    preference_video_has_clock_display: bool | None = None
    preference_video_ir_brightness: int | None = None
    #: Night-mode *preference*: one of ``"auto"``, ``"on"`` or ``"off"``
    #: (default ``"auto"``). This is what ``update-settings`` writes.
    preference_video_night_mode: str | None = None


class SettingsState(HarborMQTTPayload):
    """Runtime state attached to a settings event.

    Read-only observations owned by the device. Nothing here can be written;
    use :class:`Settings` for that.
    """

    application_state: int | None = None
    network_bars: int | None = None
    stream_state: int | None = None
    temperature: float | None = None
    #: Whether IR night vision is engaged *right now*. Device-driven: under
    #: the ``"auto"`` preference it flips on its own as light levels change,
    #: so it reflects the camera, not the last command sent.
    video_night_mode: bool | None = None
    volume_baseline_current: float | None = None
    volume_baseline_reference: float | None = None
    volume_threshold_effective: float | None = None


class SettingsEvent(HarborMQTTPayload):
    """Payload for a settings event."""

    client: str | None = None
    errors: list[Any] = Field(default_factory=list)
    is_updating: bool | None = Field(default=None, alias="isUpdating")
    seq: str | None = None
    settings: Settings | None = None
    state: SettingsState | None = None
    status: str | None = None
    triggered_by: str | None = Field(default=None, alias="triggeredBy")
    updated: dict[str, Any] = Field(default_factory=dict)


class GetCameraSettingsRequest(HarborMQTTPayload):
    """Payload for the get-settings camera command."""

    seq: str
    client: str
    triggered_by: str = Field(alias="triggeredBy")


class UpdateCameraSettingsRequest(HarborMQTTPayload):
    """Payload for the update-settings camera command.

    ``settings`` carries only the preference keys being changed; the camera
    merges them into its existing configuration and echoes back the applied
    subset.
    """

    seq: str
    settings: dict[str, Any]
    client: str
    triggered_by: str = Field(alias="triggeredBy")


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
    """Payload for a ``motion-detected`` event.

    Keys arrive in snake_case. ``duration`` is a unit-suffixed string such as
    ``"10s"`` -- never parse it as a bare number, and never treat it as a hold
    time; see :class:`NoiseDetectedEvent` for why.
    """

    active_config: str | None = None
    duration: str | None = None
    file_duration: str | None = None
    filename: str | None = None
    level: str | None = None
    sensitivity: str | None = None
    threshold: str | None = None
    thumbnail: str | None = None
    timestamp: str | None = None


class NoiseDetectedEvent(HarborMQTTPayload):
    """Payload for a ``sound-anomaly-detected`` event.

    The firmware topic calls this a sound anomaly; the app presents it as an
    alert for "sudden, loud sounds" and "sustained noises", hence the noise
    naming used here.

    Field set verified against a live camera. The audio fields are dB strings
    (``"-36.401137dB"``) and pair with the ``volume_baseline_current`` /
    ``volume_baseline_reference`` / ``volume_threshold_effective`` values on
    :class:`SettingsState`.

    ``duration`` is a unit-suffixed string (``"10s"``) that matches
    ``file_duration``, the length of the recorded clip named by ``filename``.
    It describes a detection window that has already closed by the time this
    message is published, so it must not be used to decide how long the
    detection "stays on" -- the camera never publishes a cleared counterpart at
    all.
    """

    active_config: str | None = None
    baseline: str | None = None
    baseline_reference: str | None = None
    duration: str | None = None
    file_duration: str | None = None
    filename: str | None = None
    level: str | None = None
    sensitivity: str | None = None
    threshold: str | None = None
    thumbnail: str | None = None
    timestamp: str | None = None
    user_offset: str | None = None
