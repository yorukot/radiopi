import argparse
import contextlib
import io
import json
import math
from dataclasses import dataclass

import numpy as np
from gnuradio import analog
from gnuradio import blocks
from gnuradio import fft
from gnuradio import filter
from gnuradio import gr
from gnuradio import iio
from gnuradio.filter import firdes

from nfm import DEFAULT_AUDIO_RATE
from nfm import DEFAULT_BUFFER_SIZE
from nfm import DEFAULT_FREQ
from nfm import DEFAULT_QUAD_RATE
from nfm import DEFAULT_SAMPLE_RATE
from nfm import DEFAULT_URI


def db10(value: float) -> float:
    return 10.0 * math.log10(max(value, 1e-20))


def db20(value: float) -> float:
    return 20.0 * math.log10(max(value, 1e-20))


@dataclass
class CaptureConfig:
    uri: str
    freq: int
    sample_rate: int
    quad_rate: int
    audio_rate: int
    duration: float
    buffer_size: int


class DebugCapture:
    def __init__(self, config: CaptureConfig) -> None:
        self.config = config
        self.tb = gr.top_block("RF Debug Capture")

        iq_samples = max(1, int(config.quad_rate * config.duration))
        audio_samples = max(1, int(config.audio_rate * config.duration))

        self.source = iio.fmcomms2_source_fc32(
            config.uri, [True, True, False, False], config.buffer_size
        )
        self.source.set_len_tag_key("packet_len")
        self.source.set_samplerate(config.sample_rate)
        self.source.set_frequency(config.freq)
        self.source.set_gain_mode(0, "slow_attack")
        self.source.set_quadrature(True)
        self.source.set_rfdc(True)
        self.source.set_bbdc(True)
        self.source.set_filter_params("Auto", "", 0.0, 0.0)

        self.resampler = filter.rational_resampler_ccc(
            config.quad_rate, config.sample_rate
        )
        self.channel_filter = filter.fir_filter_ccf(
            1,
            firdes.low_pass(
                1.0,
                config.quad_rate,
                10_000,
                2_000,
                fft.window.WIN_HAMMING,
            ),
        )
        with contextlib.redirect_stdout(io.StringIO()):
            self.nbfm = analog.nbfm_rx(
                audio_rate=config.audio_rate,
                quad_rate=config.quad_rate,
                tau=75e-6,
                max_dev=5_000,
            )

        self.iq_head = blocks.head(gr.sizeof_gr_complex, iq_samples)
        self.iq_sink = blocks.vector_sink_c()
        self.audio_head = blocks.head(gr.sizeof_float, audio_samples)
        self.audio_sink = blocks.vector_sink_f()

        self.tb.connect(self.source, self.resampler, self.channel_filter)
        self.tb.connect(self.channel_filter, self.iq_head, self.iq_sink)
        self.tb.connect(
            self.channel_filter, self.nbfm, self.audio_head, self.audio_sink
        )

    def run(self) -> tuple[np.ndarray, np.ndarray]:
        self.tb.run()
        iq = np.asarray(self.iq_sink.data(), dtype=np.complex64)
        audio = np.asarray(self.audio_sink.data(), dtype=np.float32)
        return iq, audio


def summarize_windows_iq(
    iq: np.ndarray, quad_rate: int, window_ms: int
) -> list[dict[str, float]]:
    window_len = max(1, int(quad_rate * window_ms / 1000.0))
    rows = []
    for idx in range(0, len(iq), window_len):
        chunk = iq[idx : idx + window_len]
        if len(chunk) == 0:
            continue
        power = float(np.mean(np.abs(chunk) ** 2))
        rows.append(
            {
                "start_s": idx / quad_rate,
                "end_s": (idx + len(chunk)) / quad_rate,
                "iq_power_db": db10(power),
            }
        )
    return rows


def summarize_windows_audio(
    audio: np.ndarray, audio_rate: int, window_ms: int
) -> list[dict[str, float]]:
    window_len = max(1, int(audio_rate * window_ms / 1000.0))
    rows = []
    for idx in range(0, len(audio), window_len):
        chunk = audio[idx : idx + window_len]
        if len(chunk) == 0:
            continue
        rms = float(np.sqrt(np.mean(chunk * chunk)))
        peak = float(np.max(np.abs(chunk))) if len(chunk) else 0.0
        rows.append(
            {
                "start_s": idx / audio_rate,
                "end_s": (idx + len(chunk)) / audio_rate,
                "audio_rms_dbfs": db20(rms),
                "audio_peak_dbfs": db20(peak),
            }
        )
    return rows


def percentile(values: np.ndarray, q: float) -> float:
    if values.size == 0:
        return float("nan")
    return float(np.percentile(values, q))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure PlutoSDR RF power and demod audio levels for squelch debugging."
    )
    parser.add_argument(
        "--uri", default=DEFAULT_URI, help=f"PlutoSDR URI (default: {DEFAULT_URI})"
    )
    parser.add_argument(
        "--freq", type=int, default=DEFAULT_FREQ, help="Center frequency in Hz"
    )
    parser.add_argument(
        "--sample-rate",
        type=int,
        default=DEFAULT_SAMPLE_RATE,
        help="SDR sample rate in samples/sec",
    )
    parser.add_argument(
        "--quad-rate",
        type=int,
        default=DEFAULT_QUAD_RATE,
        help="Post-resample complex rate",
    )
    parser.add_argument(
        "--audio-rate",
        type=int,
        default=DEFAULT_AUDIO_RATE,
        help="Demodulated audio rate",
    )
    parser.add_argument(
        "--duration", type=float, default=3.0, help="Capture duration in seconds"
    )
    parser.add_argument(
        "--window-ms",
        type=int,
        default=250,
        help="Per-window stats interval in milliseconds",
    )
    parser.add_argument(
        "--buffer-size",
        type=int,
        default=DEFAULT_BUFFER_SIZE,
        help="PlutoSDR RX buffer size",
    )
    parser.add_argument(
        "--label",
        default="run",
        help="Label printed in the summary, e.g. idle or active",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON summary",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = CaptureConfig(
        uri=args.uri,
        freq=args.freq,
        sample_rate=args.sample_rate,
        quad_rate=args.quad_rate,
        audio_rate=args.audio_rate,
        duration=args.duration,
        buffer_size=args.buffer_size,
    )

    capture = DebugCapture(config)
    iq, audio = capture.run()

    iq_window_stats = summarize_windows_iq(iq, config.quad_rate, args.window_ms)
    audio_window_stats = summarize_windows_audio(
        audio, config.audio_rate, args.window_ms
    )

    iq_db = np.array([row["iq_power_db"] for row in iq_window_stats], dtype=np.float32)
    audio_db = np.array(
        [row["audio_rms_dbfs"] for row in audio_window_stats], dtype=np.float32
    )

    summary = {
        "label": args.label,
        "freq_hz": args.freq,
        "duration_s": args.duration,
        "window_ms": args.window_ms,
        "iq_samples": int(iq.size),
        "audio_samples": int(audio.size),
        "iq_avg_db": float(
            db10(float(np.mean(np.abs(iq) ** 2))) if iq.size else float("nan")
        ),
        "iq_peak_db": float(
            db20(float(np.max(np.abs(iq)))) if iq.size else float("nan")
        ),
        "iq_p50_db": percentile(iq_db, 50),
        "iq_p95_db": percentile(iq_db, 95),
        "audio_rms_dbfs": float(
            db20(float(np.sqrt(np.mean(audio * audio)))) if audio.size else float("nan")
        ),
        "audio_peak_dbfs": float(
            db20(float(np.max(np.abs(audio)))) if audio.size else float("nan")
        ),
        "audio_p50_dbfs": percentile(audio_db, 50),
        "audio_p95_dbfs": percentile(audio_db, 95),
        "windows": [
            {**iq_row, **audio_row}
            for iq_row, audio_row in zip(
                iq_window_stats, audio_window_stats, strict=False
            )
        ],
    }

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    print(f"Label         : {summary['label']}")
    print(f"Frequency     : {summary['freq_hz'] / 1e6:.3f} MHz")
    print(f"Duration      : {summary['duration_s']:.2f} s")
    print(f"Window        : {summary['window_ms']} ms")
    print(f"IQ Avg        : {summary['iq_avg_db']:.2f} dB")
    print(f"IQ Peak       : {summary['iq_peak_db']:.2f} dB")
    print(f"IQ p50 / p95  : {summary['iq_p50_db']:.2f} / {summary['iq_p95_db']:.2f} dB")
    print(f"Audio RMS     : {summary['audio_rms_dbfs']:.2f} dBFS")
    print(f"Audio Peak    : {summary['audio_peak_dbfs']:.2f} dBFS")
    print(
        f"Audio p50/p95 : {summary['audio_p50_dbfs']:.2f} / {summary['audio_p95_dbfs']:.2f} dBFS"
    )
    print()
    print("Per-window stats:")
    for row in summary["windows"]:
        print(
            f"  {row['start_s']:5.2f}-{row['end_s']:5.2f}s"
            f" | IQ {row['iq_power_db']:7.2f} dB"
            f" | Audio RMS {row['audio_rms_dbfs']:7.2f} dBFS"
            f" | Audio Peak {row['audio_peak_dbfs']:7.2f} dBFS"
        )

    print()
    print("Debug tip:")
    print(
        "  Run once with no transmission (`--label idle`) and once while transmitting (`--label active`)."
    )
    print(
        "  Compare the IQ p95 dB values; place `--rf-squelch-db` between idle and active."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
