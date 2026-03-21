import wave
from pathlib import Path


def _write_silence(dst: wave.Wave_write, frame_count: int, bytes_per_frame: int) -> None:
    if frame_count <= 0:
        return
    chunk_frames = 8192
    silence = b"\x00" * (chunk_frames * bytes_per_frame)
    remaining = frame_count
    while remaining > 0:
        frames = min(remaining, chunk_frames)
        dst.writeframesraw(silence[: frames * bytes_per_frame])
        remaining -= frames


def merge_wavs(
    segments: list[dict],
    output_path: str,
    window_start_ms: int | None = None,
    window_duration_ms: int | None = None,
) -> None:
    if not segments:
        raise ValueError("cannot merge empty segment list")
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    params = None
    cursor_frame = 0
    sample_rate = None
    bytes_per_frame = None
    sorted_segments = sorted(segments, key=lambda item: (item["segment_start_utc_ms"], item["sequence"]))
    base_start_ms = window_start_ms if window_start_ms is not None else sorted_segments[0]["segment_start_utc_ms"]
    with wave.open(output_path, "wb") as dst:
        for index, segment in enumerate(sorted_segments):
            path = segment["wav_path"]
            with wave.open(path, "rb") as src:
                current = src.getparams()
                if index == 0:
                    params = current
                    dst.setparams(current)
                    sample_rate = current.framerate
                    bytes_per_frame = current.sampwidth * current.nchannels
                elif params is not None and (
                    current.nchannels,
                    current.sampwidth,
                    current.framerate,
                    current.comptype,
                ) != (
                    params.nchannels,
                    params.sampwidth,
                    params.framerate,
                    params.comptype,
                ):
                    raise ValueError(f"incompatible WAV params for {path}")
                assert sample_rate is not None
                assert bytes_per_frame is not None
                target_frame = max(0, int((segment["segment_start_utc_ms"] - base_start_ms) * sample_rate / 1000))
                if target_frame > cursor_frame:
                    _write_silence(dst, target_frame - cursor_frame, bytes_per_frame)
                    cursor_frame = target_frame
                frame_count = src.getnframes()
                dst.writeframes(src.readframes(frame_count))
                cursor_frame += frame_count
        if window_duration_ms is not None and sample_rate is not None and bytes_per_frame is not None:
            final_frame = max(0, int(window_duration_ms * sample_rate / 1000))
            if final_frame > cursor_frame:
                _write_silence(dst, final_frame - cursor_frame, bytes_per_frame)


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
