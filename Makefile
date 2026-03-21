UV := uv

.PHONY: help record debug-idle debug-active debug-json edge core-api core-worker check

help:
	@printf "Targets:\n"
	@printf "  make record       Run NFM recorder\n"
	@printf "  make debug-idle   Measure idle RF/audio levels\n"
	@printf "  make debug-active Measure active RF/audio levels\n"
	@printf "  make debug-json   Emit debug stats as JSON\n"
	@printf "  make edge         Run edge capture service\n"
	@printf "  make core-api     Run server ingest API\n"
	@printf "  make core-worker  Run ASR/window worker\n"
	@printf "  make check        Validate Python entrypoints\n"

record:
	$(UV) run nfm-record

debug-idle:
	$(UV) run rf-power-debug --duration 3 --window-ms 250 --label idle

debug-active:
	$(UV) run rf-power-debug --duration 3 --window-ms 250 --label active

debug-json:
	$(UV) run rf-power-debug --duration 3 --window-ms 250 --label run --json

edge:
	$(UV) run radio-edge-run --config examples/edge-config.yaml

core-api:
	$(UV) run radio-core-api --config examples/core-config.yaml

core-worker:
	$(UV) run radio-core-worker --config examples/core-config.yaml

check:
	$(UV) run python -m py_compile nfm.py rf_debug.py src/radiocommon/*.py src/radioedge/*.py src/radiocore/*.py
	$(UV) run nfm-record --help
	$(UV) run rf-power-debug --help
