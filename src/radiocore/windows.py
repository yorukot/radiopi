import wave
from pathlib import Path


def merge_wavs(segment_paths: list[str], output_path: str) -> None:
    if not segment_paths:
        raise ValueError("cannot merge empty segment list")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    params = None
    with wave.open(output_path, "wb") as dst:
        for index, path in enumerate(segment_paths):
            with wave.open(path, "rb") as src:
                current = src.getparams()
                if index == 0:
                    params = current
                    dst.setparams(current)
                elif current[:4] != params[:4]:
                    raise ValueError(f"incompatible WAV params for {path}")
                dst.writeframes(src.readframes(src.getnframes()))


def srt_timestamp(ms_value: int) -> str:
    hours, rem = divmod(ms_value, 3_600_000)
    minutes, rem = divmod(rem, 60_000)
    seconds, millis = divmod(rem, 1000)
    return f"{hours:02}:{minutes:02}:{seconds:02},{millis:03}"


def write_srt(items: list[dict], window_start_ms: int, output_path: str) -> None:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for idx, item in enumerate(items, start=1):
        start_ms = max(0, item["abs_start_ms"] - window_start_ms)
        end_ms = max(start_ms, item["abs_end_ms"] - window_start_ms)
        lines.extend(
            [
                str(idx),
                f"{srt_timestamp(start_ms)} --> {srt_timestamp(end_ms)}",
                item["text"].strip(),
                "",
            ]
        )
    output.write_text("\n".join(lines), encoding="utf-8")
