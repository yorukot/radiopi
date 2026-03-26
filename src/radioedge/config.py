from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from pathlib import Path
from typing import Any

from radiocommon import load_config
from nfm import DEFAULT_AUDIO_GAIN
from nfm import DEFAULT_AUDIO_HPF_CUTOFF
from nfm import DEFAULT_AUDIO_RATE
from nfm import DEFAULT_BUFFER_SIZE
from nfm import DEFAULT_CTCSS_FREQ
from nfm import DEFAULT_CTCSS_Q
from nfm import DEFAULT_FM_SQUELCH_THRESHOLD
from nfm import DEFAULT_FREQ
from nfm import DEFAULT_QUAD_RATE
from nfm import DEFAULT_RF_SQUELCH_ALPHA
from nfm import DEFAULT_RF_SQUELCH_DB
from nfm import DEFAULT_SAMPLE_RATE
from nfm import DEFAULT_URI


@dataclass(slots=True)
class CaptureSettings:
    uri: str = DEFAULT_URI
    freq_hz: int = DEFAULT_FREQ
    center_freq_hz: int | None = None
    sample_rate: int = DEFAULT_SAMPLE_RATE
    quad_rate: int = DEFAULT_QUAD_RATE
    audio_rate: int = DEFAULT_AUDIO_RATE
    buffer_size: int = DEFAULT_BUFFER_SIZE
    rf_squelch_db: float = DEFAULT_RF_SQUELCH_DB
    rf_squelch_alpha: float = DEFAULT_RF_SQUELCH_ALPHA
    fm_squelch_threshold: float = DEFAULT_FM_SQUELCH_THRESHOLD
    audio_hpf_cutoff: float = DEFAULT_AUDIO_HPF_CUTOFF
    ctcss_freq: float = DEFAULT_CTCSS_FREQ
    ctcss_q: float = DEFAULT_CTCSS_Q
    audio_gain: float = DEFAULT_AUDIO_GAIN


@dataclass(slots=True)
class SegmentSettings:
    stream_id: str = "pluto430230"
    duration_sec: int = 10
    start_threshold: float = 0.02
    stop_threshold: float = 0.01
    min_silence_ms: int = 1200
    min_segment_ms: int = 500
    max_segment_sec: int = 300


@dataclass(slots=True)
class SpoolSettings:
    root_dir: str = "./spool"
    keep_acked: bool = True
    scan_interval_sec: float = 2.0


@dataclass(slots=True)
class UploadSettings:
    base_url: str = "http://127.0.0.1:8080"
    api_key: str = "change-me"
    timeout_sec: float = 30.0
    initial_backoff_sec: float = 2.0
    max_backoff_sec: float = 60.0


@dataclass(slots=True)
class RuntimeSettings:
    runtime_dir: str = "./runtime"
    health_path: str = "./runtime/edge-status.json"
    fifo_name: str = "capture.pcm"
    status_interval_sec: float = 5.0


@dataclass(slots=True)
class ChannelSettings:
    freq_hz: int
    stream_id: str
    ctcss_type: str = "Tone"
    ctcss_freq: float = DEFAULT_CTCSS_FREQ
    modulation: str = "FM"
    priority: str = "High"


@dataclass(slots=True)
class EdgeConfig:
    capture: CaptureSettings
    segment: SegmentSettings
    spool: SpoolSettings
    upload: UploadSettings
    runtime: RuntimeSettings
    channels: list[ChannelSettings] = field(default_factory=list)

    @classmethod
    def from_file(cls, path: str | Path) -> "EdgeConfig":
        raw = load_config(path)
        return cls(
            capture=_build_settings(CaptureSettings, raw.get("capture", {})),
            segment=_build_settings(SegmentSettings, raw.get("segment", {})),
            spool=_build_settings(SpoolSettings, raw.get("spool", {})),
            upload=_build_settings(UploadSettings, raw.get("upload", {})),
            runtime=_build_settings(RuntimeSettings, raw.get("runtime", {})),
            channels=[
                _build_settings(ChannelSettings, item)
                for item in raw.get("channels", [])
            ],
        )


def _build_settings(settings_type: type[Any], raw: dict[str, Any]) -> Any:
    allowed = {item.name for item in fields(settings_type)}
    return settings_type(**{key: value for key, value in raw.items() if key in allowed})
