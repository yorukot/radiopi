import httpx

from .config import TelegramSettings


class TelegramNotifier:
    def __init__(self, settings: TelegramSettings) -> None:
        self.settings = settings

    def send_message(self, text: str, message_thread_id: int | None = None) -> None:
        if not self.settings.enabled:
            return
        base_url = f"https://api.telegram.org/bot{self.settings.bot_token}"
        payload = {"chat_id": self.settings.chat_id, "text": text[:4096]}
        if message_thread_id is not None:
            payload["message_thread_id"] = message_thread_id
        with httpx.Client(timeout=self.settings.timeout_sec) as client:
            response = client.post(
                f"{base_url}/sendMessage",
                data=payload,
            )
            response.raise_for_status()
