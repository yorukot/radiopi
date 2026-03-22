import argparse
import json
import os
import sqlite3
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi import File
from fastapi import Form
from fastapi import Header
from fastapi import HTTPException
from fastapi import UploadFile

from .config import CoreConfig
from .db import Database


def create_app(config: CoreConfig) -> FastAPI:
    db = Database(config.database.path)
    app = FastAPI(title="radio-core")

    for directory in [
        config.data.root_dir,
        config.data.raw_dir,
        config.data.raw_asr_dir,
        config.data.transcripts_dir,
        config.data.archives_dir,
    ]:
        Path(directory).mkdir(parents=True, exist_ok=True)

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    @app.get("/readyz")
    def readyz() -> dict:
        snapshot = db.stats_snapshot()
        worker_health = None
        worker_path = Path(config.data.worker_health_path)
        if worker_path.exists():
            worker_health = json.loads(worker_path.read_text(encoding="utf-8"))
        return {"ok": True, **snapshot, "worker": worker_health}

    @app.post("/v1/segments")
    async def ingest_segment(
        file: UploadFile = File(...),
        metadata: str = Form(...),
        x_api_key: str | None = Header(default=None),
    ) -> dict:
        if x_api_key != config.api.api_key:
            raise HTTPException(status_code=401, detail="invalid api key")
        try:
            payload = json.loads(metadata)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail=f"invalid metadata json: {exc}") from exc
        required = {
            "session_id",
            "stream_id",
            "segment_id",
            "sequence",
            "segment_start_utc_ms",
            "duration_ms",
            "sample_rate",
            "channels",
            "sample_format",
            "freq_hz",
        }
        missing = sorted(required - payload.keys())
        if missing:
            raise HTTPException(status_code=400, detail=f"missing metadata fields: {', '.join(missing)}")
        if db.segment_exists(payload["segment_id"]):
            return {
                "accepted": True,
                "segment_id": payload["segment_id"],
                "status": "duplicate",
            }
        raw_dir = Path(config.data.raw_dir)
        wav_tmp = raw_dir / f"{payload['segment_id']}.wav.tmp"
        wav_path = raw_dir / f"{payload['segment_id']}.wav"
        metadata_tmp = raw_dir / f"{payload['segment_id']}.json.tmp"
        metadata_path = raw_dir / f"{payload['segment_id']}.json"
        content = await file.read()
        with wav_tmp.open("wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        wav_tmp.replace(wav_path)
        metadata_tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        try:
            db.insert_received_segment(
                payload,
                str(wav_path),
                str(metadata_path),
                config.asr.window_sec * 1000,
            )
        except sqlite3.IntegrityError:
            wav_path.unlink(missing_ok=True)
            metadata_tmp.unlink(missing_ok=True)
            return {
                "accepted": True,
                "segment_id": payload["segment_id"],
                "status": "duplicate",
            }
        metadata_tmp.replace(metadata_path)
        return {"accepted": True, "segment_id": payload["segment_id"], "status": "received"}

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the radio-core ingest API")
    parser.add_argument("--config", required=True, help="Path to server YAML or JSON config")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = CoreConfig.from_file(args.config)
    uvicorn.run(create_app(config), host=config.api.host, port=config.api.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
