import argparse
import contextlib
import io
import logging
from pathlib import Path

from scipy import signal
from gnuradio import analog
from gnuradio import blocks
from gnuradio import fft
from gnuradio import filter
from gnuradio import gr
from gnuradio import iio
from gnuradio.filter import firdes


DEFAULT_URI = "usb:1.2.5"
DEFAULT_FREQ = 430_230_000
DEFAULT_SAMPLE_RATE = 1_008_000
DEFAULT_QUAD_RATE = 240_000
DEFAULT_AUDIO_RATE = 16_000
DEFAULT_DURATION = 10
DEFAULT_BUFFER_SIZE = 262_144
DEFAULT_OUTPUT = "record.wav"
DEFAULT_RF_SQUELCH_DB = -30.0
DEFAULT_RF_SQUELCH_ALPHA = 1e-3
DEFAULT_FM_SQUELCH_THRESHOLD = -1.0
DEFAULT_AUDIO_HPF_CUTOFF = 300.0
DEFAULT_CTCSS_FREQ = 156.7
DEFAULT_CTCSS_Q = 30.0
DEFAULT_AUDIO_GAIN = 0.7


def design_audio_hpf_taps(audio_rate: int, cutoff_hz: float) -> list[float]:
    transition_hz = 100.0
    attenuation_db = 90.0
    width = transition_hz / (audio_rate / 2.0)
    ntaps, beta = signal.kaiserord(attenuation_db, width)
    if ntaps % 2 == 0:
        ntaps += 1
    taps = signal.firwin(
        ntaps,
        cutoff_hz,
        fs=audio_rate,
        pass_zero=False,
        window=("kaiser", beta),
    )
    return taps.tolist()


def design_ctcss_notch_taps(audio_rate: int, tone_hz: float, q_factor: float) -> list[float]:
    if q_factor <= 0:
        raise ValueError("ctcss_q must be greater than zero")
    stop_width_hz = max(3.0, tone_hz / (2.0 * q_factor))
    taps = signal.firwin(
        1201,
        [max(1.0, tone_hz - stop_width_hz), tone_hz + stop_width_hz],
        fs=audio_rate,
        pass_zero="bandstop",
        window=("kaiser", 8.6),
    )
    return taps.tolist()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("pluto-gnuradio-recorder")


class BaseNfmFlowgraph:
    def __init__(
        self,
        uri: str,
        freq: int,
        sample_rate: int,
        quad_rate: int,
        audio_rate: int,
        buffer_size: int,
        rf_squelch_db: float,
        rf_squelch_alpha: float,
        fm_squelch_threshold: float,
        audio_hpf_cutoff: float,
        ctcss_freq: float,
        ctcss_q: float,
        audio_gain: float,
    ) -> None:
        self.tb = gr.top_block("Pluto NFM Recorder")

        if quad_rate % audio_rate != 0:
            raise ValueError("quad_rate must be an integer multiple of audio_rate")

        if ctcss_freq <= 0:
            raise ValueError("ctcss_freq must be greater than zero")

        log.info(
            "Initializing NFM flowgraph freq=%.3fMHz sample_rate=%.3fMHz quad_rate=%.1fkHz audio_rate=%sHz",
            freq / 1e6,
            sample_rate / 1e6,
            quad_rate / 1e3,
            audio_rate,
        )
        log.info(
            "Flowgraph filters rf_squelch=%s fm_squelch=%s ctcss=%.1fHz(q=%.1f) audio_hpf=%.1fHz gain=%.2f",
            f"{rf_squelch_db:.1f}dB" if rf_squelch_db >= -50.0 else "disabled",
            f"{fm_squelch_threshold:.2f}" if fm_squelch_threshold >= 0.0 else "disabled",
            ctcss_freq,
            ctcss_q,
            audio_hpf_cutoff,
            audio_gain,
        )
        log.info("Connecting to PlutoSDR: %s", uri)
        self.source = iio.fmcomms2_source_fc32(
            uri, [True, True, False, False], buffer_size
        )
        self.source.set_len_tag_key("packet_len")
        self.source.set_samplerate(sample_rate)
        self.source.set_frequency(freq)
        self.source.set_gain_mode(0, "slow_attack")
        self.source.set_quadrature(True)
        self.source.set_rfdc(True)
        self.source.set_bbdc(True)
        self.source.set_filter_params("Auto", "", 0.0, 0.0)
        log.info("Configured PlutoSDR source buffer_size=%s", buffer_size)

        resample_interp = quad_rate
        resample_decim = sample_rate
        self.resampler = filter.rational_resampler_ccc(resample_interp, resample_decim)

        self.channel_filter = filter.fir_filter_ccf(
            1,
            firdes.low_pass(
                1.0,
                quad_rate,
                10_000,
                2_000,
                fft.window.WIN_HAMMING,
            ),
        )
        self.rf_squelch = None
        if rf_squelch_db >= -50.0:
            self.rf_squelch = analog.simple_squelch_cc(rf_squelch_db, rf_squelch_alpha)

        with contextlib.redirect_stdout(io.StringIO()):
            self.nbfm = analog.nbfm_rx(
                audio_rate=audio_rate,
                quad_rate=quad_rate,
                tau=75e-6,
                max_dev=5_000,
            )
        self.audio_hpf = filter.fir_filter_fff(
            1,
            design_audio_hpf_taps(audio_rate, audio_hpf_cutoff),
        )
        self.ctcss_notch = filter.fir_filter_fff(
            1,
            design_ctcss_notch_taps(audio_rate, ctcss_freq, ctcss_q),
        )
        self.audio_gain = blocks.multiply_const_ff(audio_gain)

        if self.rf_squelch is not None:
            self.tb.connect(
                self.source,
                self.resampler,
                self.channel_filter,
                self.rf_squelch,
                self.nbfm,
            )
        else:
            self.tb.connect(self.source, self.resampler, self.channel_filter, self.nbfm)
        if fm_squelch_threshold >= 0.0:
            self.fm_squelch = analog.standard_squelch(audio_rate)
            self.fm_squelch.set_threshold(fm_squelch_threshold)
            self.tb.connect(
                self.nbfm,
                self.fm_squelch,
                self.ctcss_notch,
                self.audio_hpf,
                self.audio_gain,
            )
        else:
            self.fm_squelch = None
            self.tb.connect(
                self.nbfm,
                self.ctcss_notch,
                self.audio_hpf,
                self.audio_gain,
            )
        log.info("NFM flowgraph ready")

    def connect_audio_sink(self, sink) -> None:
        self.tb.connect(self.audio_gain, sink)

    def run(self) -> None:
        log.info("Starting GNU Radio flowgraph")
        self.tb.run()
        log.info("GNU Radio flowgraph stopped")

    def stop(self) -> None:
        log.info("Stopping GNU Radio flowgraph")
        self.tb.stop()

    def wait(self) -> None:
        self.tb.wait()


class NfmRecorder(BaseNfmFlowgraph):
    def __init__(
        self,
        uri: str,
        freq: int,
        sample_rate: int,
        quad_rate: int,
        audio_rate: int,
        duration: int,
        output_path: str,
        buffer_size: int,
        rf_squelch_db: float,
        rf_squelch_alpha: float,
        fm_squelch_threshold: float,
        audio_hpf_cutoff: float,
        ctcss_freq: float,
        ctcss_q: float,
        audio_gain: float,
    ) -> None:
        duration_samples = int(duration * audio_rate)
        if duration_samples <= 0:
            raise ValueError("duration must be greater than zero")

        output = Path(output_path)
        if output.exists():
            output.unlink()

        super().__init__(
            uri=uri,
            freq=freq,
            sample_rate=sample_rate,
            quad_rate=quad_rate,
            audio_rate=audio_rate,
            buffer_size=buffer_size,
            rf_squelch_db=rf_squelch_db,
            rf_squelch_alpha=rf_squelch_alpha,
            fm_squelch_threshold=fm_squelch_threshold,
            audio_hpf_cutoff=audio_hpf_cutoff,
            ctcss_freq=ctcss_freq,
            ctcss_q=ctcss_q,
            audio_gain=audio_gain,
        )

        self.head = blocks.head(gr.sizeof_float, duration_samples)
        self.wav_sink = blocks.wavfile_sink(
            str(output),
            1,
            audio_rate,
            blocks.FORMAT_WAV,
            blocks.FORMAT_PCM_16,
            False,
        )
        self.tb.connect(self.audio_gain, self.head, self.wav_sink)


class ContinuousPcmCapture(BaseNfmFlowgraph):
    def __init__(
        self,
        uri: str,
        freq: int,
        sample_rate: int,
        quad_rate: int,
        audio_rate: int,
        pcm_path: str,
        buffer_size: int,
        rf_squelch_db: float,
        rf_squelch_alpha: float,
        fm_squelch_threshold: float,
        audio_hpf_cutoff: float,
        ctcss_freq: float,
        ctcss_q: float,
        audio_gain: float,
    ) -> None:
        super().__init__(
            uri=uri,
            freq=freq,
            sample_rate=sample_rate,
            quad_rate=quad_rate,
            audio_rate=audio_rate,
            buffer_size=buffer_size,
            rf_squelch_db=rf_squelch_db,
            rf_squelch_alpha=rf_squelch_alpha,
            fm_squelch_threshold=fm_squelch_threshold,
            audio_hpf_cutoff=audio_hpf_cutoff,
            ctcss_freq=ctcss_freq,
            ctcss_q=ctcss_q,
            audio_gain=audio_gain,
        )

        self.float_to_short = blocks.float_to_short(1, 32767.0)
        self.pcm_sink = blocks.file_sink(gr.sizeof_short, pcm_path, False)
        self.pcm_sink.set_unbuffered(True)
        self.connect_audio_sink(self.float_to_short)
        self.tb.connect(self.float_to_short, self.pcm_sink)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Record 10 seconds of NFM audio from PlutoSDR to WAV."
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
        help="Intermediate FM demod rate",
    )
    parser.add_argument(
        "--audio-rate",
        type=int,
        default=DEFAULT_AUDIO_RATE,
        help="Output WAV sample rate",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help="Recording duration in seconds",
    )
    parser.add_argument(
        "--buffer-size",
        type=int,
        default=DEFAULT_BUFFER_SIZE,
        help="PlutoSDR RX buffer size",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output WAV path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--rf-squelch-db",
        type=float,
        default=DEFAULT_RF_SQUELCH_DB,
        help="Mute RF below this baseband power threshold in dB; values below -50 disable RF squelch",
    )
    parser.add_argument(
        "--rf-squelch-alpha",
        type=float,
        default=DEFAULT_RF_SQUELCH_ALPHA,
        help="Baseband squelch smoothing factor",
    )
    parser.add_argument(
        "--fm-squelch-threshold",
        type=float,
        default=DEFAULT_FM_SQUELCH_THRESHOLD,
        help="FM noise squelch threshold for demodulated audio; set negative to disable",
    )
    parser.add_argument(
        "--audio-hpf-cutoff",
        type=float,
        default=DEFAULT_AUDIO_HPF_CUTOFF,
        help="High-pass cutoff in Hz for removing sub-audible tones",
    )
    parser.add_argument(
        "--ctcss-freq",
        type=float,
        default=DEFAULT_CTCSS_FREQ,
        help="CTCSS notch center frequency in Hz",
    )
    parser.add_argument(
        "--ctcss-q",
        type=float,
        default=DEFAULT_CTCSS_Q,
        help="CTCSS notch Q factor",
    )
    parser.add_argument(
        "--audio-gain",
        type=float,
        default=DEFAULT_AUDIO_GAIN,
        help="Linear audio gain after filtering",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    log.info("Recorder settings:")
    log.info("  URI         : %s", args.uri)
    log.info("  Frequency   : %.3f MHz", args.freq / 1e6)
    log.info("  Sample Rate : %.3f MHz", args.sample_rate / 1e6)
    log.info("  Quad Rate   : %.1f kHz", args.quad_rate / 1e3)
    log.info("  Audio Rate  : %d Hz", args.audio_rate)
    log.info("  Duration    : %d s", args.duration)
    if args.rf_squelch_db >= -50.0:
        log.info("  RF Squelch  : %.1f dB", args.rf_squelch_db)
    else:
        log.info("  RF Squelch  : disabled")
    if args.fm_squelch_threshold >= 0.0:
        log.info("  FM Squelch  : %.2f", args.fm_squelch_threshold)
    else:
        log.info("  FM Squelch  : disabled")
    log.info("  Audio HPF   : %.1f Hz", args.audio_hpf_cutoff)
    log.info("  CTCSS Notch : %.1f Hz (Q=%.1f)", args.ctcss_freq, args.ctcss_q)
    log.info("  Audio Gain  : %.1f", args.audio_gain)
    log.info("  Output      : %s", args.output)

    tb = NfmRecorder(
        uri=args.uri,
        freq=args.freq,
        sample_rate=args.sample_rate,
        quad_rate=args.quad_rate,
        audio_rate=args.audio_rate,
        duration=args.duration,
        output_path=args.output,
        buffer_size=args.buffer_size,
        rf_squelch_db=args.rf_squelch_db,
        rf_squelch_alpha=args.rf_squelch_alpha,
        fm_squelch_threshold=args.fm_squelch_threshold,
        audio_hpf_cutoff=args.audio_hpf_cutoff,
        ctcss_freq=args.ctcss_freq,
        ctcss_q=args.ctcss_q,
        audio_gain=args.audio_gain,
    )

    try:
        log.info("Recording started")
        tb.run()
        tb.wav_sink.close()
        log.info("Recording finished: %s", args.output)
        return 0
    except KeyboardInterrupt:
        log.warning("Recording interrupted by user")
        tb.stop()
        tb.wait()
        tb.wav_sink.close()
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
