import logging
import socket
from urllib.parse import urlparse
from pathlib import Path

import httpx

from radiocommon import now_utc_ms

from .config import SpoolSettings
from .config import UploadSettings

log = logging.getLogger(__name__)


class SegmentUploader:
    def __init__(
        self,
        spool: SpoolSettings,
        upload: UploadSettings,
        stop_event,
        stats: dict,
    ) -> None:
        self.spool = spool
        self.upload = upload
        self.stop_event = stop_event
        self.stats = stats
        self.failure_counts: dict[str, int] = {}

    def _endpoint_host(self) -> str:
        parsed = urlparse(self.upload.base_url)
        if not parsed.hostname:
            raise ValueError(f"Invalid upload.base_url: {self.upload.base_url!r}")
        return parsed.hostname

    def _check_dns(self) -> bool:
        host = self._endpoint_host()
        try:
            socket.getaddrinfo(host, None)
        except socket.gaierror as exc:
            self.stats["upload_dns_ok"] = False
            self.stats["last_upload_dns_error"] = str(exc)
            log.warning(
                "Upload host %s does not resolve right now: %s",
                host,
                exc,
            )
            return False
        self.stats["upload_dns_ok"] = True
        self.stats.pop("last_upload_dns_error", None)
        return True

    def run(self) -> None:
        root = Path(self.spool.root_dir)
        ready_dir = root / "ready"
        sending_dir = root / "sending"
        acked_dir = root / "acked"
        for directory in [
            root,
            ready_dir,
            sending_dir,
            acked_dir,
            root / "failed",
            root / "tmp",
        ]:
            directory.mkdir(parents=True, exist_ok=True)
        self.stats["uploader_running"] = True
        self.stats["upload_target"] = self.upload.base_url
        with httpx.Client(timeout=self.upload.timeout_sec) as client:
            while not self.stop_event.is_set():
                dns_ready = self._check_dns()
                uploaded_any = False
                for meta_path in sorted(ready_dir.glob("*.json")):
                    wav_path = meta_path.with_suffix(".wav")
                    if not wav_path.exists():
                        continue
                    if not dns_ready:
                        break
                    sending_meta = sending_dir / meta_path.name
                    sending_wav = sending_dir / wav_path.name
                    try:
                        meta_path.replace(sending_meta)
                        wav_path.replace(sending_wav)
                    except FileNotFoundError:
                        continue
                    uploaded_any = True
                    segment_id = sending_meta.stem
                    self.stats["last_upload_segment_id"] = segment_id
                    self.stats["last_upload_started_at"] = now_utc_ms()
                    log.info(
                        "Starting upload for segment=%s target=%s",
                        segment_id,
                        self.upload.base_url,
                    )
                    if self._upload_one(client, sending_wav, sending_meta):
                        self.failure_counts.pop(segment_id, None)
                        self.stats["last_upload_success_at"] = now_utc_ms()
                        if self.spool.keep_acked:
                            sending_meta.replace(acked_dir / sending_meta.name)
                            sending_wav.replace(acked_dir / sending_wav.name)
                            log.info(
                                "Upload complete for segment=%s; moved to acked",
                                segment_id,
                            )
                        else:
                            sending_meta.unlink(missing_ok=True)
                            sending_wav.unlink(missing_ok=True)
                            log.info(
                                "Upload complete for segment=%s; local copy removed",
                                segment_id,
                            )
                    else:
                        tries = self.failure_counts.get(segment_id, 0) + 1
                        self.failure_counts[segment_id] = tries
                        self.stats["last_upload_failed_at"] = now_utc_ms()
                        self.stats["last_upload_attempts"] = tries
                        sending_meta.replace(ready_dir / sending_meta.name)
                        sending_wav.replace(ready_dir / sending_wav.name)
                        delay = min(
                            self.upload.initial_backoff_sec * (2 ** (tries - 1)),
                            self.upload.max_backoff_sec,
                        )
                        log.warning(
                            "Upload failed for %s, retrying in %.1fs", segment_id, delay
                        )
                        if self.stop_event.wait(delay):
                            break
                self._update_spool_stats(root)
                if not uploaded_any:
                    self.stop_event.wait(self.spool.scan_interval_sec)
        self.stats["uploader_running"] = False

    def _upload_one(
        self, client: httpx.Client, wav_path: Path, meta_path: Path
    ) -> bool:
        metadata = meta_path.read_text(encoding="utf-8")
        headers = {"X-API-Key": self.upload.api_key}
        try:
            with wav_path.open("rb") as handle:
                response = client.post(
                    f"{self.upload.base_url.rstrip('/')}/v1/segments",
                    headers=headers,
                    files={
                        "file": (wav_path.name, handle, "audio/wav"),
                        "metadata": (None, metadata, "application/json"),
                    },
                )
            response.raise_for_status()
            payload = response.json()
            log.info(
                "Uploaded segment=%s status=%s accepted=%s",
                wav_path.stem,
                payload.get("status"),
                payload.get("accepted"),
            )
            return bool(payload.get("accepted"))
        except httpx.ConnectError as exc:
            cause = exc.__cause__
            if isinstance(cause, socket.gaierror):
                log.warning(
                    "Failed to upload %s because upload host DNS lookup failed: %s",
                    wav_path.name,
                    cause,
                )
            else:
                log.exception("Failed to upload %s: %s", wav_path.name, exc)
            return False
        except Exception as exc:
            log.exception("Failed to upload %s: %s", wav_path.name, exc)
            return False

    def _update_spool_stats(self, root: Path) -> None:
        ready_dir = root / "ready"
        ready_files = list(ready_dir.glob("*.json"))
        self.stats["spool_ready_count"] = len(ready_files)
        total_bytes = 0
        for path in root.rglob("*"):
            if path.is_file():
                total_bytes += path.stat().st_size
        self.stats["spool_disk_usage"] = total_bytes
