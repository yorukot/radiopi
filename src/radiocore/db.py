import json
import sqlite3
from pathlib import Path

from radiocommon import iso_utc_from_ms, now_utc_ms, window_start_ms


SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions(
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  session_start_utc_ms INTEGER NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segments(
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  segment_start_utc_ms INTEGER NOT NULL,
  duration_ms INTEGER NOT NULL,
  wav_path TEXT NOT NULL,
  metadata_path TEXT NOT NULL,
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  asr_json_path TEXT,
  error_text TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transcript_items(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  segment_id TEXT NOT NULL,
  rel_start_ms INTEGER NOT NULL,
  rel_end_ms INTEGER NOT NULL,
  abs_start_ms INTEGER NOT NULL,
  abs_end_ms INTEGER NOT NULL,
  text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS windows(
  id TEXT PRIMARY KEY,
  stream_id TEXT NOT NULL,
  window_start_utc_ms INTEGER NOT NULL,
  wav_path TEXT,
  srt_path TEXT,
  telegram_status TEXT NOT NULL,
  telegram_error_text TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    def insert_received_segment(
        self,
        metadata: dict,
        wav_path: str,
        metadata_path: str,
        window_ms: int,
    ) -> str:
        now_iso = iso_utc_from_ms(now_utc_ms())
        window_ms_value = window_start_ms(metadata["segment_start_utc_ms"], window_ms)
        window_id = f"{metadata['stream_id']}-{window_ms_value}"
        session_start_utc_ms = metadata.get("session_start_utc_ms", metadata["segment_start_utc_ms"])
        with self.connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO sessions(id, stream_id, session_start_utc_ms, created_at) VALUES(?, ?, ?, ?)",
                (
                    metadata["session_id"],
                    metadata["stream_id"],
                    session_start_utc_ms,
                    now_iso,
                ),
            )
            conn.execute(
                "INSERT INTO segments(id, session_id, stream_id, sequence, segment_start_utc_ms, duration_ms, wav_path, metadata_path, status, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?)",
                (
                    metadata["segment_id"],
                    metadata["session_id"],
                    metadata["stream_id"],
                    metadata["sequence"],
                    metadata["segment_start_utc_ms"],
                    metadata["duration_ms"],
                    wav_path,
                    metadata_path,
                    now_iso,
                    now_iso,
                ),
            )
            conn.execute(
                "INSERT OR IGNORE INTO windows(id, stream_id, window_start_utc_ms, telegram_status, created_at, updated_at) VALUES(?, ?, ?, 'pending', ?, ?)",
                (window_id, metadata["stream_id"], window_ms_value, now_iso, now_iso),
            )
        return window_id

    def segment_exists(self, segment_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT 1 FROM segments WHERE id = ?", (segment_id,)).fetchone()
        return row is not None

    def pop_next_queued_segment(self):
        now_iso = iso_utc_from_ms(now_utc_ms())
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM segments WHERE status = 'queued' ORDER BY segment_start_utc_ms LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE segments SET status = 'transcribing', attempts = attempts + 1, updated_at = ? WHERE id = ?",
                (now_iso, row["id"]),
            )
            return dict(row)

    def complete_transcription(self, segment_id: str, asr_json_path: str, items: list[dict]) -> None:
        now_iso = iso_utc_from_ms(now_utc_ms())
        with self.connect() as conn:
            conn.executemany(
                "INSERT INTO transcript_items(segment_id, rel_start_ms, rel_end_ms, abs_start_ms, abs_end_ms, text) VALUES(?, ?, ?, ?, ?, ?)",
                [
                    (
                        segment_id,
                        item["rel_start_ms"],
                        item["rel_end_ms"],
                        item["abs_start_ms"],
                        item["abs_end_ms"],
                        item["text"],
                    )
                    for item in items
                ],
            )
            conn.execute(
                "UPDATE segments SET status = 'transcribed', asr_json_path = ?, error_text = NULL, updated_at = ? WHERE id = ?",
                (asr_json_path, now_iso, segment_id),
            )

    def fail_segment(self, segment_id: str, error_text: str) -> None:
        now_iso = iso_utc_from_ms(now_utc_ms())
        with self.connect() as conn:
            conn.execute(
                "UPDATE segments SET status = 'failed', error_text = ?, updated_at = ? WHERE id = ?",
                (error_text, now_iso, segment_id),
            )

    def append_daily_jsonl(self, transcripts_dir: str | Path, items: list[dict]) -> None:
        root = Path(transcripts_dir)
        root.mkdir(parents=True, exist_ok=True)
        by_date: dict[str, list[dict]] = {}
        for item in items:
            date_key = iso_utc_from_ms(item["abs_start_ms"])[:10]
            by_date.setdefault(date_key, []).append(item)
        for date_key, rows in by_date.items():
            path = root / f"{date_key}.jsonl"
            with path.open("a", encoding="utf-8") as handle:
                for row in rows:
                    payload = {
                        "abs_start_utc": iso_utc_from_ms(row["abs_start_ms"]),
                        "abs_end_utc": iso_utc_from_ms(row["abs_end_ms"]),
                        "text": row["text"],
                        "segment_id": row["segment_id"],
                    }
                    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def list_buildable_windows(self, now_ms: int, window_ms: int, close_grace_ms: int) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM windows WHERE srt_path IS NULL AND ? >= window_start_utc_ms + ? + ?",
                (now_ms, window_ms, close_grace_ms),
            ).fetchall()
            result = []
            for row in rows:
                open_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM segments WHERE stream_id = ? AND segment_start_utc_ms >= ? AND segment_start_utc_ms < ? AND status NOT IN ('transcribed', 'window_built', 'notified')",
                    (
                        row["stream_id"],
                        row["window_start_utc_ms"],
                        row["window_start_utc_ms"] + window_ms,
                    ),
                ).fetchone()["c"]
                total_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM segments WHERE stream_id = ? AND segment_start_utc_ms >= ? AND segment_start_utc_ms < ?",
                    (
                        row["stream_id"],
                        row["window_start_utc_ms"],
                        row["window_start_utc_ms"] + window_ms,
                    ),
                ).fetchone()["c"]
                if total_count > 0 and open_count == 0:
                    result.append(dict(row))
        return result

    def fetch_window_segments(self, stream_id: str, window_start: int, window_ms: int) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM segments WHERE stream_id = ? AND segment_start_utc_ms >= ? AND segment_start_utc_ms < ? ORDER BY segment_start_utc_ms, sequence",
                (stream_id, window_start, window_start + window_ms),
            ).fetchall()
        return [dict(row) for row in rows]

    def fetch_window_transcript_items(self, segment_ids: list[str]) -> list[dict]:
        if not segment_ids:
            return []
        placeholders = ",".join("?" for _ in segment_ids)
        with self.connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM transcript_items WHERE segment_id IN ({placeholders}) ORDER BY abs_start_ms, abs_end_ms, id",
                tuple(segment_ids),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_window_built(self, window_id: str, wav_path: str, srt_path: str, window_ms: int) -> None:
        now_iso = iso_utc_from_ms(now_utc_ms())
        with self.connect() as conn:
            conn.execute(
                "UPDATE windows SET wav_path = ?, srt_path = ?, updated_at = ? WHERE id = ?",
                (wav_path, srt_path, now_iso, window_id),
            )
            conn.execute(
                "UPDATE segments SET status = 'window_built', updated_at = ? WHERE stream_id = (SELECT stream_id FROM windows WHERE id = ?) AND segment_start_utc_ms >= (SELECT window_start_utc_ms FROM windows WHERE id = ?) AND segment_start_utc_ms < (SELECT window_start_utc_ms + ? FROM windows WHERE id = ?)",
                (now_iso, window_id, window_id, window_ms, window_id),
            )

    def list_pending_notifications(self) -> list[dict]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM windows WHERE srt_path IS NOT NULL AND telegram_status = 'pending' ORDER BY window_start_utc_ms"
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_window_notified(self, window_id: str, window_ms: int) -> None:
        now_iso = iso_utc_from_ms(now_utc_ms())
        with self.connect() as conn:
            conn.execute(
                "UPDATE windows SET telegram_status = 'sent', telegram_error_text = NULL, updated_at = ? WHERE id = ?",
                (now_iso, window_id),
            )
            conn.execute(
                "UPDATE segments SET status = 'notified', updated_at = ? WHERE stream_id = (SELECT stream_id FROM windows WHERE id = ?) AND segment_start_utc_ms >= (SELECT window_start_utc_ms FROM windows WHERE id = ?) AND segment_start_utc_ms < (SELECT window_start_utc_ms + ? FROM windows WHERE id = ?)",
                (now_iso, window_id, window_id, window_ms, window_id),
            )

    def mark_window_notify_failed(self, window_id: str, error_text: str) -> None:
        now_iso = iso_utc_from_ms(now_utc_ms())
        with self.connect() as conn:
            conn.execute(
                "UPDATE windows SET telegram_status = 'failed', telegram_error_text = ?, updated_at = ? WHERE id = ?",
                (error_text, now_iso, window_id),
            )

    def stats_snapshot(self) -> dict:
        with self.connect() as conn:
            queue_depth = conn.execute(
                "SELECT COUNT(*) AS c FROM segments WHERE status IN ('queued', 'transcribing')"
            ).fetchone()["c"]
            last_ingested = conn.execute(
                "SELECT id, segment_start_utc_ms FROM segments ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            last_transcribed = conn.execute(
                "SELECT id, updated_at FROM segments WHERE status IN ('transcribed', 'window_built', 'notified') ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        return {
            "queue_depth": queue_depth,
            "last_ingested_segment": dict(last_ingested) if last_ingested else None,
            "last_transcribed_segment": dict(last_transcribed) if last_transcribed else None,
        }
