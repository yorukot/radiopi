# RadioPi Services

This repo now contains two Python services for the WAV-first SDR to ASR pipeline:

- `radio-edge-run`: PlutoSDR capture, continuous PCM output, activity-based WAV segmentation, local spool, retry upload
- `radio-core-api`: ingest API and health endpoints on the GPU server
- `radio-core-worker`: persistent `faster-whisper` worker, transcript normalization, 10-minute WAV/SRT archive build, Telegram notification

## Requirements

- PlutoSDR and GNU Radio with `gr-iio` for the edge node
- Python managed with `uv`
- CUDA-capable GPU for `faster-whisper` if you want GPU inference

## Install

```bash
uv sync
```

## Edge Service

Example config: `examples/edge-config.yaml`

Run it with:

```bash
uv run radio-edge-run --config examples/edge-config.yaml
```

What it does:

- keeps the GNU Radio flowgraph running continuously
- emits raw PCM16 at `16000 Hz` into a FIFO
- opens a new WAV when audio activity starts and closes it after silence
- writes `wav + json` into `spool/ready`
- uploads to the server with retry and backoff
- writes runtime status to `runtime/edge-status.json`

Spool layout:

```text
spool/
  tmp/
  ready/
  sending/
  acked/
  failed/
```

## Core Service

Example config: `examples/core-config.yaml`

Start the API:

```bash
uv run radio-core-api --config examples/core-config.yaml
```

Start the worker:

```bash
uv run radio-core-worker --config examples/core-config.yaml
```

### Caddy Reverse Proxy

If you want a simple public-facing proxy in front of `radio-core-api`, use Caddy.

1. Copy `Caddyfile.example` to `Caddyfile` and replace `your-domain.example.com` with your real domain.
2. Keep the API listening on `127.0.0.1:8080` or `0.0.0.0:8080`.
3. Start the API and worker with `uv`.
4. Start Caddy with the config.

Example commands:

```bash
cp Caddyfile.example Caddyfile
uv run radio-core-api --config core-config.yaml
uv run radio-core-worker --config core-config.yaml
caddy run --config Caddyfile
```

If you want Caddy to be the only public entrypoint, it is a good idea to bind the API to localhost in `core-config.yaml`:

```yaml
api:
  host: "127.0.0.1"
  port: 8080
```

## Docker

Two separate container images are available for the core services:

- `Dockerfile.core-api` for the ingest API
- `Dockerfile.core-worker` for the ASR worker on a CUDA host

Build them:

```bash
docker build -f Dockerfile.core-api -t radiopi-core-api .
docker build -f Dockerfile.core-worker -t radiopi-core-worker .
```

Run the API:

```bash
docker run --rm \
  -p 8080:8080 \
  -v "$(pwd)/core-config.yaml:/config/core-config.yaml:ro" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/db.sqlite3:/app/db.sqlite3" \
  radiopi-core-api
```

Run the CUDA worker:

```bash
docker run --rm \
  --gpus all \
  -v "$(pwd)/core-config.yaml:/config/core-config.yaml:ro" \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/db.sqlite3:/app/db.sqlite3" \
  radiopi-core-worker
```

Notes:

- both containers expect the config at `/config/core-config.yaml`
- keep `database.path` and `data.*` inside the config aligned with the mounted `/app/db.sqlite3` and `/app/data`
- the worker image is based on `nvidia/cuda` and needs NVIDIA Container Toolkit plus `docker run --gpus all`
- if you want CPU-only inference in the worker container, set `asr.device: "cpu"` in `core-config.yaml`

What the server does:

- accepts `POST /v1/segments` with `multipart/form-data`
- stores WAV and metadata durably before acknowledging
- queues jobs in SQLite
- keeps one `faster-whisper` model loaded in memory
- pins `faster-whisper` to Chinese (`asr.language: "zh"`) instead of auto-detecting
- converts `faster-whisper` Chinese output from Simplified to Traditional with OpenCC (`asr.opencc_config`)
- writes raw ASR JSON and canonical daily JSONL transcripts
- builds 10-minute merged WAV and SRT archives
- optionally sends each transcribed line to Telegram as a plain chat message

Health endpoints:

- `GET /healthz`
- `GET /readyz`

Artifacts:

```text
data/
  raw/
  raw_asr/
  transcripts/
  archives/
```

Telegram delivery:

- when `telegram.enabled: true`, the worker sends each non-empty transcribed line as its own Telegram message
- the worker only uses `sendMessage`; it does not upload SRT or WAV files

## Smoke Test Flow

1. Start `radio-core-api`
2. Start `radio-core-worker`
3. Start `radio-edge-run`
4. Watch `spool/acked`, `data/raw`, `data/raw_asr`, and `data/archives`

Edge segmentation is now activity-based instead of fixed 10-second chunks. Segment start and duration are derived from the continuous sample clock so server-side archives can preserve real silence gaps and keep WAV/SRT timelines aligned.

## Legacy Tools

The original one-shot recorder and debug helper still exist:

- `uv run nfm-record`
- `uv run rf-power-debug`

The recorder default audio rate is now `16000 Hz`, and the CTCSS notch now actually uses `ctcss_q`.

## Make Targets

```bash
make help
make record
make debug-idle
make debug-active
make debug-json
make edge
make core-api
make core-worker
make check
```
