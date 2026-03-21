import json
import logging
import os
import threading
import wave
from pathlib import Path

from radiocommon import format_session_id, now_utc_ms

from .config import CaptureSettings
from .config import SegmentSettings

log = logging.getLogger(__name__)


class SegmentWriter:
    def __init__(
        self,
        fifo_path: str | Path,
        spool_root: str | Path,
        capture: CaptureSettings,
        segment: SegmentSettings,
        stop_event: threading.Event,
        stats: dict,
    ) -> None:
        self.fifo_path = Path(fifo_path)
        self.spool_root = Path(spool_root)
        self.capture = capture
        self.segment = segment
        self.stop_event = stop_event
        self.stats = stats
        self.ready_dir = self.spool_root / "ready"
        self.tmp_dir = self.spool_root / "tmp"
        self.bytes_per_sample = 2
        self.bytes_per_segment = (
            self.capture.audio_rate * self.segment.duration_sec * self.bytes_per_sample
        )
        self.session_start_utc_ms = now_utc_ms()
        self.session_id = format_session_id(
            self.segment.stream_id,
            self.session_start_utc_ms,
        )
        self.sequence = 0

    def run(self) -> None:
        for directory in [
            self.spool_root,
            self.spool_root / "tmp",
            self.spool_root / "ready",
            self.spool_root / "sending",
            self.spool_root / "acked",
            self.spool_root / "failed",
        ]:
            directory.mkdir(parents=True, exist_ok=True)

        if self.fifo_path.exists():
            self.fifo_path.unlink()
        os.mkfifo(self.fifo_path)
        self.stats["segmenter_running"] = True
        self.stats["session_id"] = self.session_id
        self.stats["session_start_utc_ms"] = self.session_start_utc_ms
        buffer = bytearray()
        log.info("Segment writer waiting on FIFO %s", self.fifo_path)
        try:
            with self.fifo_path.open("rb", buffering=0) as handle:
                while not self.stop_event.is_set():
                    chunk = handle.read(4096)
                    if not chunk:
                        continue
                    buffer.extend(chunk)
                    while len(buffer) >= self.bytes_per_segment:
                        payload = bytes(buffer[: self.bytes_per_segment])
                        del buffer[: self.bytes_per_segment]
                        self._flush_segment(payload)
        finally:
            self.stats["segmenter_running"] = False
            if self.fifo_path.exists():
                self.fifo_path.unlink()

    def _flush_segment(self, payload: bytes) -> None:
        segment_start_utc_ms = self.session_start_utc_ms + self.sequence * self.segment.duration_sec * 1000
        segment_id = (
            f"{self.segment.stream_id}-{segment_start_utc_ms}-{self.sequence:06d}"
        )
        wav_tmp = self.tmp_dir / f"{segment_id}.wav.tmp"
        wav_path = self.ready_dir / f"{segment_id}.wav"
        meta_tmp = self.tmp_dir / f"{segment_id}.json.tmp"
        meta_path = self.ready_dir / f"{segment_id}.json"
        with wave.open(str(wav_tmp), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(self.bytes_per_sample)
            wav_file.setframerate(self.capture.audio_rate)
            wav_file.writeframes(payload)

        metadata = {
            "session_id": self.session_id,
            "stream_id": self.segment.stream_id,
            "segment_id": segment_id,
            "sequence": self.sequence,
            "segment_start_utc_ms": segment_start_utc_ms,
            "duration_ms": self.segment.duration_sec * 1000,
            "sample_rate": self.capture.audio_rate,
            "channels": 1,
            "sample_format": "s16le",
            "freq_hz": self.capture.freq_hz,
        }
        meta_tmp.write_text(json.dumps(metadata, ensure_ascii=True, indent=2), encoding="utf-8")
        wav_tmp.replace(wav_path)
        meta_tmp.replace(meta_path)
        self.stats["last_segment_sequence"] = self.sequence
        self.stats["last_segment_id"] = segment_id
        self.sequence += 1
