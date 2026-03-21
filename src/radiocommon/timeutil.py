from datetime import UTC, datetime


def now_utc_ms() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


def iso_utc_from_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000.0, tz=UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def format_session_id(stream_id: str, start_ms: int) -> str:
    stamp = datetime.fromtimestamp(start_ms / 1000.0, tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{stream_id}"


def window_start_ms(timestamp_ms: int, window_ms: int) -> int:
    return timestamp_ms - (timestamp_ms % window_ms)
