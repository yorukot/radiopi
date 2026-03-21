import json
import logging
import math
import os
import stat
import threading
import wave
from pathlib import Path

import numpy as np

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
        reader_ready_event: threading.Event | None = None,
    ) -> None:
        self.fifo_path = Path(fifo_path)
        self.spool_root = Path(spool_root)
        self.capture = capture
        self.segment = segment
        self.stop_event = stop_event
        self.stats = stats
        self.reader_ready_event = reader_ready_event
        self.ready_dir = self.spool_root / "ready"
        self.tmp_dir = self.spool_root / "tmp"
        self.bytes_per_sample = 2
        self.session_start_utc_ms = now_utc_ms()
        self.session_id = format_session_id(
            self.segment.stream_id,
            self.session_start_utc_ms,
        )
        self.sequence = 0
        self.total_samples = 0
        self.pending_bytes = bytearray()
        self.start_threshold = self._validate_threshold(
            self.segment.start_threshold, "start_threshold"
        )
        self.stop_threshold = self._validate_threshold(
            self.segment.stop_threshold, "stop_threshold"
        )
        if self.stop_threshold > self.start_threshold:
            raise ValueError(
                "stop_threshold must be less than or equal to start_threshold"
            )
        if self.segment.min_silence_ms <= 0:
            raise ValueError("min_silence_ms must be greater than zero")
        if self.segment.min_segment_ms <= 0:
            raise ValueError("min_segment_ms must be greater than zero")
        if self.segment.max_segment_sec <= 0:
            raise ValueError("max_segment_sec must be greater than zero")
        self.min_silence_samples = max(
            1, int(self.capture.audio_rate * self.segment.min_silence_ms / 1000)
        )
        self.min_segment_samples = max(
            1, int(self.capture.audio_rate * self.segment.min_segment_ms / 1000)
        )
        self.max_segment_samples = max(
            1, int(self.capture.audio_rate * self.segment.max_segment_sec)
        )
        self.current_segment: dict | None = None
        self.stats["signal_active"] = False
        self.level_log_interval_ms = 2000
        self.last_level_log_ms = 0
        self.last_no_pcm_log_ms = 0
        self.seen_pcm = False

    def _validate_threshold(self, value: float, name: str) -> float:
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"{name} must be between 0.0 and 1.0")
        return value

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

        if not self.fifo_path.exists():
            os.mkfifo(self.fifo_path)
            log.info("Created missing capture FIFO %s", self.fifo_path)
        else:
            fifo_mode = self.fifo_path.stat().st_mode
            if not stat.S_ISFIFO(fifo_mode):
                raise RuntimeError(f"Capture path is not a FIFO: {self.fifo_path}")
        self.stats["segmenter_running"] = True
        self.stats["session_id"] = self.session_id
        self.stats["session_start_utc_ms"] = self.session_start_utc_ms
        self.stats["activity_start_threshold"] = self.start_threshold
        self.stats["activity_stop_threshold"] = self.stop_threshold
        self.stats["activity_min_silence_ms"] = self.segment.min_silence_ms
        log.info("Segment writer waiting on FIFO %s", self.fifo_path)
        fifo_fd = os.open(self.fifo_path, os.O_RDONLY | os.O_NONBLOCK)
        if self.reader_ready_event is not None:
            self.reader_ready_event.set()
        try:
            while not self.stop_event.is_set():
                try:
                    chunk = os.read(fifo_fd, 4096)
                except BlockingIOError:
                    chunk = b""
                if not chunk:
                    self._maybe_log_no_pcm()
                    self.stop_event.wait(0.1)
                    continue
                if not self.seen_pcm:
                    self.seen_pcm = True
                    log.info("Received first PCM chunk from capture FIFO")
                self._process_chunk(chunk)
        finally:
            if self.reader_ready_event is not None:
                self.reader_ready_event.set()
            os.close(fifo_fd)
            if self.current_segment is not None:
                self._close_segment()
            self.stats["segmenter_running"] = False
            if self.fifo_path.exists():
                self.fifo_path.unlink()

    def _process_chunk(self, chunk: bytes) -> None:
        self.pending_bytes.extend(chunk)
        usable_bytes = len(self.pending_bytes) - (
            len(self.pending_bytes) % self.bytes_per_sample
        )
        if usable_bytes <= 0:
            return
        payload = bytes(self.pending_bytes[:usable_bytes])
        del self.pending_bytes[:usable_bytes]
        chunk_samples = len(payload) // self.bytes_per_sample
        raw_chunk_start_sample = self.total_samples
        raw_chunk_end_sample = raw_chunk_start_sample + chunk_samples
        chunk_start_sample = raw_chunk_start_sample
        chunk_end_sample = raw_chunk_end_sample
        threshold = (
            self.stop_threshold
            if self.current_segment is not None
            else self.start_threshold
        )
        active_offsets = self._active_offsets(payload, threshold)
        active = active_offsets is not None
        level_dbfs = self._chunk_level_dbfs(payload)
        self.stats["current_level_dbfs"] = round(level_dbfs, 1)
        self._maybe_log_level(level_dbfs, active)

        if self.current_segment is None and active:
            start_offset, _ = active_offsets
            chunk_start_sample += start_offset
            payload = payload[start_offset * self.bytes_per_sample :]
            chunk_samples = len(payload) // self.bytes_per_sample
            chunk_end_sample = chunk_start_sample + chunk_samples
            self._open_segment(chunk_start_sample)

        if self.current_segment is not None:
            self.current_segment["wav_file"].writeframes(payload)
            self.current_segment["samples_written"] += chunk_samples
            self.current_segment["last_sample"] = chunk_end_sample
            if active:
                _, last_offset = active_offsets
                self.current_segment["last_active_sample"] = (
                    raw_chunk_start_sample + last_offset + 1
                )
            silence_samples = (
                chunk_end_sample - self.current_segment["last_active_sample"]
            )
            reached_max = (
                self.current_segment["samples_written"] >= self.max_segment_samples
            )
            if silence_samples >= self.min_silence_samples or reached_max:
                self._close_segment()

        self.total_samples = raw_chunk_end_sample

    def _active_offsets(
        self, payload: bytes, threshold: float
    ) -> tuple[int, int] | None:
        samples = np.frombuffer(payload, dtype="<i2").astype(np.int32)
        if samples.size == 0:
            return None
        limit = int(threshold * 32767.0)
        active_indexes = np.flatnonzero(np.abs(samples) >= limit)
        if active_indexes.size == 0:
            return None
        return int(active_indexes[0]), int(active_indexes[-1])

    def _chunk_level_dbfs(self, payload: bytes) -> float:
        samples = np.frombuffer(payload, dtype="<i2").astype(np.int32)
        if samples.size == 0:
            return -120.0
        peak = int(np.max(np.abs(samples)))
        if peak <= 0:
            return -120.0
        return max(-120.0, 20.0 * math.log10(peak / 32767.0))

    def _maybe_log_level(self, level_dbfs: float, active: bool) -> None:
        now_ms = now_utc_ms()
        if now_ms - self.last_level_log_ms < self.level_log_interval_ms:
            return
        self.last_level_log_ms = now_ms
        log.info(
            "Audio level=%.1f dBFS signal_active=%s start_threshold=%.1f dBFS stop_threshold=%.1f dBFS",
            level_dbfs,
            active,
            self._threshold_dbfs(self.start_threshold),
            self._threshold_dbfs(self.stop_threshold),
        )

    def _maybe_log_no_pcm(self) -> None:
        now_ms = now_utc_ms()
        if now_ms - self.last_no_pcm_log_ms < self.level_log_interval_ms:
            return
        self.last_no_pcm_log_ms = now_ms
        log.info("Waiting for PCM samples from capture FIFO")

    def _threshold_dbfs(self, threshold: float) -> float:
        if threshold <= 0.0:
            return -120.0
        return max(-120.0, 20.0 * math.log10(threshold))

    def _open_segment(self, start_sample: int) -> None:
        segment_start_utc_ms = self.session_start_utc_ms + int(
            start_sample * 1000 / self.capture.audio_rate
        )
        segment_id = (
            f"{self.segment.stream_id}-{segment_start_utc_ms}-{self.sequence:06d}"
        )
        wav_tmp = self.tmp_dir / f"{segment_id}.wav.tmp"
        wav_file = wave.open(str(wav_tmp), "wb")
        wav_file.setnchannels(1)
        wav_file.setsampwidth(self.bytes_per_sample)
        wav_file.setframerate(self.capture.audio_rate)
        self.current_segment = {
            "segment_id": segment_id,
            "segment_start_utc_ms": segment_start_utc_ms,
            "start_sample": start_sample,
            "last_active_sample": start_sample,
            "last_sample": start_sample,
            "samples_written": 0,
            "wav_tmp": wav_tmp,
            "wav_path": self.ready_dir / f"{segment_id}.wav",
            "meta_tmp": self.tmp_dir / f"{segment_id}.json.tmp",
            "meta_path": self.ready_dir / f"{segment_id}.json",
            "wav_file": wav_file,
        }
        self.stats["current_segment_id"] = segment_id
        self.stats["signal_active"] = True
        self.stats["current_segment_started_at"] = segment_start_utc_ms
        log.info(
            "Signal detected; recording segment=%s start_utc_ms=%s sequence=%s",
            segment_id,
            segment_start_utc_ms,
            self.sequence,
        )

    def _close_segment(self) -> None:
        assert self.current_segment is not None
        segment = self.current_segment
        segment["wav_file"].close()
        duration_ms = int((segment["samples_written"] * 1000) / self.capture.audio_rate)
        metadata = {
            "session_id": self.session_id,
            "session_start_utc_ms": self.session_start_utc_ms,
            "stream_id": self.segment.stream_id,
            "segment_id": segment["segment_id"],
            "sequence": self.sequence,
            "segment_start_utc_ms": segment["segment_start_utc_ms"],
            "duration_ms": duration_ms,
            "sample_rate": self.capture.audio_rate,
            "channels": 1,
            "sample_format": "s16le",
            "freq_hz": self.capture.freq_hz,
        }
        if segment["samples_written"] < self.min_segment_samples:
            segment["wav_tmp"].unlink(missing_ok=True)
            segment["meta_tmp"].unlink(missing_ok=True)
            self.stats["last_discarded_segment_id"] = segment["segment_id"]
            log.info(
                "Signal ended; discarded short segment=%s duration_ms=%s threshold_ms=%s",
                segment["segment_id"],
                duration_ms,
                self.segment.min_segment_ms,
            )
        else:
            segment["meta_tmp"].write_text(
                json.dumps(metadata, ensure_ascii=True, indent=2), encoding="utf-8"
            )
            segment["wav_tmp"].replace(segment["wav_path"])
            segment["meta_tmp"].replace(segment["meta_path"])
            self.stats["last_segment_sequence"] = self.sequence
            self.stats["last_segment_id"] = segment["segment_id"]
            self.stats["last_segment_duration_ms"] = duration_ms
            self.stats["last_segment_ready_at"] = now_utc_ms()
            log.info(
                "Signal ended; finalized segment=%s duration_ms=%s ready_path=%s",
                segment["segment_id"],
                duration_ms,
                segment["wav_path"],
            )
            self.sequence += 1
        self.stats["signal_active"] = False
        self.stats.pop("current_segment_id", None)
        self.stats.pop("current_segment_started_at", None)
        self.current_segment = None
