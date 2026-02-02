from pydantic import BaseModel


class LocalLivekitHeartbeatEvent(BaseModel):
    app_start_time: str
    app_version: str
    bitrate: float
    camera_present: bool
    camera_state: str
    is_healthy: dict
    network_bars: int
    os_version: str
    receiver_present: bool
    speaker_state: str
    stream_quality: str
    stream_start_time: str
    viewers_by_identity: dict
    viewers_by_identity_full: dict


class HeartbeatEvent(BaseModel):
    app_version: str
    efuse_voltage: int
    image_sensor_temperature: float
    ntc_adc_voltage: int
    ntc_temperature: float
    os_version: str
    raw_temperature: float
    sensor_temperature: float
    temperature: float


class Settings(BaseModel):
    log_level: str
    preference_ai: dict
    preference_anomaly_configs: dict
    preference_anomaly_throttle_duration_seconds: int
    preference_auto_pinning: bool
    preference_connection_band: str
    preference_connection_bssid: str
    preference_display_name: str
    preference_moment_length: int
    preference_operating_mode: str
    preference_scheduled_reboot: str | None
    preference_silence_alerting_until: str | None
    preference_stream_paused: bool
    preference_video_clock_display_tz_abbrev: str
    preference_video_clock_display_tz_offset: int
    preference_video_flip: bool
    preference_video_has_clock_display: bool
    preference_video_ir_brightness: int
    preference_video_night_mode: str


class SettingsState(BaseModel):
    application_state: int
    network_bars: int
    stream_state: int
    temperature: float
    video_night_mode: bool


class SettingsEvent(BaseModel):
    client: str
    is_updating: bool
    seq: str
    settings: Settings
    state: SettingsState
    triggered_by: str
    updated: dict


class ViewerJoinedEvent(BaseModel):
    client: str
    identity: str
    is_local: bool
    role: str
    viewer_id: str


class ViewerLeftEvent(BaseModel):
    client: str
    identity: str
    is_local: bool
    role: str
    viewer_id: str


class MotionDetectedEvent(BaseModel):
    active_config: str
    duration: str
    filename: str
    level: str
    sensitivity: str
    threshold: str
    thumbnail: str
    timestamp: str
