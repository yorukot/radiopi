from dataclasses import dataclass
from pathlib import Path

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
    duration_sec: int = 10
    stream_id: str = "pluto430230"


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
class EdgeConfig:
    capture: CaptureSettings
    segment: SegmentSettings
    spool: SpoolSettings
    upload: UploadSettings
    runtime: RuntimeSettings

    @classmethod
    def from_file(cls, path: str | Path) -> "EdgeConfig":
        raw = load_config(path)
        return cls(
            capture=CaptureSettings(**raw.get("capture", {})),
            segment=SegmentSettings(**raw.get("segment", {})),
            spool=SpoolSettings(**raw.get("spool", {})),
            upload=UploadSettings(**raw.get("upload", {})),
            runtime=RuntimeSettings(**raw.get("runtime", {})),
        )
