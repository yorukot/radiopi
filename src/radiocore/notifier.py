from pathlib import Path

import httpx

from radiocommon import iso_utc_from_ms

from .config import TelegramSettings


class TelegramNotifier:
    def __init__(self, settings: TelegramSettings) -> None:
        self.settings = settings

    def send_window(self, window: dict, summary_text: str) -> None:
        if not self.settings.enabled:
            return
        base_url = f"https://api.telegram.org/bot{self.settings.bot_token}"
        with httpx.Client(timeout=self.settings.timeout_sec) as client:
            response = client.post(
                f"{base_url}/sendMessage",
                data={"chat_id": self.settings.chat_id, "text": summary_text[:4096]},
            )
            response.raise_for_status()
            self._send_document(client, f"{base_url}/sendDocument", Path(window["srt_path"]))
            if self.settings.send_audio and window.get("wav_path"):
                self._send_document(client, f"{base_url}/sendDocument", Path(window["wav_path"]))

    def _send_document(self, client: httpx.Client, url: str, file_path: Path) -> None:
        with file_path.open("rb") as handle:
            response = client.post(
                url,
                data={"chat_id": self.settings.chat_id},
                files={"document": (file_path.name, handle)},
            )
            response.raise_for_status()


def build_window_summary(window: dict, transcript_items: list[dict]) -> str:
    text = " ".join(item["text"].strip() for item in transcript_items if item["text"].strip())
    preview = text[:600] + ("..." if len(text) > 600 else "")
    return (
        f"Window {iso_utc_from_ms(window['window_start_utc_ms'])}\n"
        f"Items: {len(transcript_items)}\n"
        f"Preview: {preview or '(no transcript)'}"
    )
