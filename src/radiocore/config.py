from dataclasses import dataclass
from pathlib import Path

from radiocommon import load_config


@dataclass(slots=True)
class ApiSettings:
    host: str = "0.0.0.0"
    port: int = 8080
    api_key: str = "change-me"


@dataclass(slots=True)
class DatabaseSettings:
    path: str = "./db.sqlite3"


@dataclass(slots=True)
class DataSettings:
    root_dir: str = "./data"
    raw_dir: str = "./data/raw"
    raw_asr_dir: str = "./data/raw_asr"
    transcripts_dir: str = "./data/transcripts"
    archives_dir: str = "./data/archives"
    telegram_dir: str = "./data/telegram"
    worker_health_path: str = "./data/worker-status.json"


@dataclass(slots=True)
class AsrSettings:
    model: str = "turbo"
    device: str = "cuda"
    compute_type: str = "float16"
    language: str | None = None
    beam_size: int = 5
    vad_filter: bool = False
    condition_on_previous_text: bool = False
    poll_interval_sec: float = 2.0
    close_grace_sec: int = 30
    window_sec: int = 600


@dataclass(slots=True)
class TelegramSettings:
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""
    send_audio: bool = False
    timeout_sec: float = 30.0


@dataclass(slots=True)
class CoreConfig:
    api: ApiSettings
    database: DatabaseSettings
    data: DataSettings
    asr: AsrSettings
    telegram: TelegramSettings

    @classmethod
    def from_file(cls, path: str | Path) -> "CoreConfig":
        raw = load_config(path)
        return cls(
            api=ApiSettings(**raw.get("api", {})),
            database=DatabaseSettings(**raw.get("database", {})),
            data=DataSettings(**raw.get("data", {})),
            asr=AsrSettings(**raw.get("asr", {})),
            telegram=TelegramSettings(**raw.get("telegram", {})),
        )
