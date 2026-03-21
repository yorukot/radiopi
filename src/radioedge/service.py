import argparse
import json
import logging
import signal
import threading
import time
from pathlib import Path

from nfm import ContinuousPcmCapture

from .config import EdgeConfig
from .segmenter import SegmentWriter
from .uploader import SegmentUploader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


class EdgeService:
    def __init__(self, config: EdgeConfig) -> None:
        self.config = config
        self.stop_event = threading.Event()
        self.stats = {
            "capture_running": False,
            "segmenter_running": False,
            "uploader_running": False,
        }
        runtime_dir = Path(self.config.runtime.runtime_dir)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        self.fifo_path = runtime_dir / self.config.runtime.fifo_name
        log.info(
            "Preparing edge capture uri=%s freq=%.3fMHz fifo=%s",
            self.config.capture.uri,
            self.config.capture.freq_hz / 1e6,
            self.fifo_path,
        )
        self.capture = ContinuousPcmCapture(
            uri=self.config.capture.uri,
            freq=self.config.capture.freq_hz,
            sample_rate=self.config.capture.sample_rate,
            quad_rate=self.config.capture.quad_rate,
            audio_rate=self.config.capture.audio_rate,
            pcm_path=str(self.fifo_path),
            buffer_size=self.config.capture.buffer_size,
            rf_squelch_db=self.config.capture.rf_squelch_db,
            rf_squelch_alpha=self.config.capture.rf_squelch_alpha,
            fm_squelch_threshold=self.config.capture.fm_squelch_threshold,
            audio_hpf_cutoff=self.config.capture.audio_hpf_cutoff,
            ctcss_freq=self.config.capture.ctcss_freq,
            ctcss_q=self.config.capture.ctcss_q,
            audio_gain=self.config.capture.audio_gain,
        )
        log.info("Edge capture object initialized")

    def run(self) -> int:
        self._install_signal_handlers()
        log.info(
            "Starting edge service stream=%s freq=%sHz upload=%s spool=%s",
            self.config.segment.stream_id,
            self.config.capture.freq_hz,
            self.config.upload.base_url,
            self.config.spool.root_dir,
        )
        segmenter = SegmentWriter(
            fifo_path=self.fifo_path,
            spool_root=self.config.spool.root_dir,
            capture=self.config.capture,
            segment=self.config.segment,
            stop_event=self.stop_event,
            stats=self.stats,
        )
        uploader = SegmentUploader(
            spool=self.config.spool,
            upload=self.config.upload,
            stop_event=self.stop_event,
            stats=self.stats,
        )
        threads = [
            threading.Thread(target=segmenter.run, name="segmenter", daemon=True),
            threading.Thread(target=uploader.run, name="uploader", daemon=True),
            threading.Thread(target=self._write_status_loop, name="status", daemon=True),
        ]
        for thread in threads:
            thread.start()
            log.info("Started %s thread", thread.name)
        while not self.fifo_path.exists() and not self.stop_event.is_set():
            time.sleep(0.1)
        capture_thread = threading.Thread(target=self._run_capture, name="capture", daemon=True)
        capture_thread.start()
        log.info("Started capture thread")
        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        finally:
            log.info("Stopping edge service")
            self.stop_event.set()
            try:
                self.capture.stop()
                self.capture.wait()
            except Exception:
                log.exception("Capture shutdown raised an error")
            for thread in threads:
                thread.join(timeout=2)
            capture_thread.join(timeout=2)
        return 0

    def _run_capture(self) -> None:
        self.stats["capture_running"] = True
        log.info("Capture loop starting")
        try:
            self.capture.run()
        except Exception:
            self.stats["capture_running"] = False
            self.stop_event.set()
            log.exception("Capture loop failed")
            raise
        self.stats["capture_running"] = False
        log.info("Capture loop stopped")

    def _write_status_loop(self) -> None:
        status_path = Path(self.config.runtime.health_path)
        status_path.parent.mkdir(parents=True, exist_ok=True)
        while not self.stop_event.is_set():
            tmp_path = status_path.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(self.stats, ensure_ascii=True, indent=2), encoding="utf-8")
            tmp_path.replace(status_path)
            self.stop_event.wait(self.config.runtime.status_interval_sec)

    def _install_signal_handlers(self) -> None:
        def handle_signal(signum, _frame) -> None:
            log.info("Received signal %s, shutting down", signum)
            self.stop_event.set()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the RadioPi edge capture service.")
    parser.add_argument("--config", required=True, help="Path to edge YAML or JSON config")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    service = EdgeService(EdgeConfig.from_file(args.config))
    return service.run()


if __name__ == "__main__":
    raise SystemExit(main())
