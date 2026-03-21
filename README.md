# RadioPi NFM Recorder

GNU Radio based PlutoSDR NFM recorder with WAV export and an RF power debug tool for squelch tuning.

## Requirements

- PlutoSDR
- GNU Radio with `gr-iio`
- Python managed with `uv`

## Install

```bash
uv sync
```

## Record Audio

Default 10-second recording:

```bash
uv run nfm-record
```

Useful options:

```bash
uv run nfm-record --output record.wav
uv run nfm-record --rf-squelch-db -30
uv run nfm-record --audio-gain 0.7
```

Current defaults are tuned for the measured environment:

- RF squelch: `-30 dB`
- FM squelch: disabled by default
- Audio HPF: `300 Hz`
- CTCSS notch: `156.7 Hz`

## Debug Squelch

Measure idle noise floor:

```bash
uv run rf-power-debug --duration 3 --window-ms 250 --label idle
```

Measure active signal:

```bash
uv run rf-power-debug --duration 3 --window-ms 250 --label active
```

JSON output:

```bash
uv run rf-power-debug --duration 3 --window-ms 250 --label idle --json
```

How to set RF squelch:

1. Run one idle capture.
2. Run one active capture.
3. Place `--rf-squelch-db` clearly above idle `IQ p95` and clearly below active `IQ p50`.

In the current measurements:

- idle `IQ p95`: about `-43.4 dB`
- active `IQ p50`: about `-11.0 dB`
- recommended starting point: `-30 dB`

## Make Targets

```bash
make help
make record
make debug-idle
make debug-active
make debug-json
make check
```

## Project Files

- `nfm.py`: recorder entrypoint
- `rf_debug.py`: RF/audio level debug tool
- `pyproject.toml`: `uv` project config
- `Makefile`: shortcut commands
