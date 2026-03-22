import argparse
import json
import logging
import signal
import threading
from pathlib import Path

from faster_whisper import WhisperModel
from opencc import OpenCC

from radiocommon import now_utc_ms

from .config import CoreConfig
from .db import Database
from .notifier import TelegramNotifier
from .windows import merge_wavs
from .windows import write_srt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


class CoreWorker:
    def __init__(self, config: CoreConfig) -> None:
        self.config = config
        self.db = Database(config.database.path)
        self.stop_event = threading.Event()
        self.model = WhisperModel(
            self.config.asr.model,
            device=self.config.asr.device,
            compute_type=self.config.asr.compute_type,
        )
        self.opencc = OpenCC(self.config.asr.opencc_config) if self.config.asr.opencc_config else None
        self.notifier = TelegramNotifier(config.telegram)
        self.health_path = Path(config.data.worker_health_path)
        self.health_path.parent.mkdir(parents=True, exist_ok=True)

    def run(self) -> int:
        self._install_signal_handlers()
        while not self.stop_event.is_set():
            worked = False
            segment = self.db.pop_next_queued_segment()
            if segment:
                worked = True
                self._transcribe_segment(segment)
            if self._notify_ready_transcript_items():
                worked = True
            if self._build_ready_windows():
                worked = True
            self._write_health()
            if not worked:
                self.stop_event.wait(self.config.asr.poll_interval_sec)
        return 0

    def _transcribe_segment(self, segment: dict) -> None:
        log.info("Transcribing %s", segment["id"])
        try:
            segments, info = self.model.transcribe(
                segment["wav_path"],
                language=self.config.asr.language,
                beam_size=self.config.asr.beam_size,
                vad_filter=self.config.asr.vad_filter,
                condition_on_previous_text=self.config.asr.condition_on_previous_text,
                word_timestamps=True,
            )
            segment_rows = []
            raw_payload = {
                "segment_id": segment["id"],
                "language": info.language,
                "duration": info.duration,
                "segments": [],
            }
            for item in segments:
                converted_text = self._convert_text(item.text)
                raw_payload["segments"].append(
                    {
                        "id": item.id,
                        "start": item.start,
                        "end": item.end,
                        "text": converted_text,
                        "words": [
                            {
                                "word": self._convert_text(word.word),
                                "start": word.start,
                                "end": word.end,
                                "probability": word.probability,
                            }
                            for word in (item.words or [])
                        ],
                    }
                )
                text = converted_text.strip()
                if not text:
                    continue
                rel_start_ms = int(item.start * 1000)
                rel_end_ms = int(item.end * 1000)
                segment_rows.append(
                    {
                        "segment_id": segment["id"],
                        "rel_start_ms": rel_start_ms,
                        "rel_end_ms": rel_end_ms,
                        "abs_start_ms": segment["segment_start_utc_ms"] + rel_start_ms,
                        "abs_end_ms": segment["segment_start_utc_ms"] + rel_end_ms,
                        "text": text,
                    }
                )
            asr_path = Path(self.config.data.raw_asr_dir) / f"{segment['id']}.json"
            asr_path.parent.mkdir(parents=True, exist_ok=True)
            asr_path.write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            self.db.complete_transcription(segment["id"], str(asr_path), segment_rows)
            self.db.append_daily_jsonl(self.config.data.transcripts_dir, segment_rows)
        except Exception as exc:
            log.exception("ASR failed for %s", segment["id"])
            self.db.fail_segment(segment["id"], str(exc))

    def _build_ready_windows(self) -> bool:
        worked = False
        window_ms = self.config.asr.window_sec * 1000
        close_grace_ms = self.config.asr.close_grace_sec * 1000
        for window in self.db.list_buildable_windows(now_utc_ms(), window_ms, close_grace_ms):
            segments = self.db.fetch_window_segments(
                window["stream_id"],
                window["window_start_utc_ms"],
                window_ms,
            )
            transcript_items = self.db.fetch_window_transcript_items([segment["id"] for segment in segments])
            archive_dir = Path(self.config.data.archives_dir)
            archive_dir.mkdir(parents=True, exist_ok=True)
            base_name = window["id"]
            wav_path = archive_dir / f"{base_name}.wav"
            srt_path = archive_dir / f"{base_name}.srt"
            merge_wavs(
                segments,
                str(wav_path),
                window_start_ms=window["window_start_utc_ms"],
                window_duration_ms=window_ms,
            )
            write_srt(transcript_items, window["window_start_utc_ms"], str(srt_path))
            self.db.mark_window_built(window["id"], str(wav_path), str(srt_path), window_ms)
            worked = True
        return worked

    def _notify_ready_transcript_items(self) -> bool:
        worked = False
        for item in self.db.list_pending_transcript_notifications():
            text = item["text"].strip()
            if not text:
                self.db.mark_transcript_item_notified(item["id"])
                continue
            try:
                self.notifier.send_message(text)
                self.db.mark_transcript_item_notified(item["id"])
            except Exception as exc:
                log.exception("Telegram send failed for transcript item %s", item["id"])
                self.db.mark_transcript_item_notify_failed(item["id"], str(exc))
            worked = True
        return worked

    def _write_health(self) -> None:
        snapshot = self.db.stats_snapshot()
        snapshot["gpu_worker_loaded_model"] = self.config.asr.model
        snapshot["updated_at_ms"] = now_utc_ms()
        tmp_path = self.health_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(snapshot, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp_path.replace(self.health_path)

    def _convert_text(self, text: str) -> str:
        if not text or self.opencc is None:
            return text
        return self.opencc.convert(text)

    def _install_signal_handlers(self) -> None:
        def handle_signal(signum, _frame) -> None:
            log.info("Received signal %s, stopping worker", signum)
            self.stop_event.set()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the radio-core ASR worker")
    parser.add_argument("--config", required=True, help="Path to server YAML or JSON config")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    worker = CoreWorker(CoreConfig.from_file(args.config))
    return worker.run()


if __name__ == "__main__":
    raise SystemExit(main())
