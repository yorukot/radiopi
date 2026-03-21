UV := uv

.PHONY: help record debug-idle debug-active debug-json check

help:
	@printf "Targets:\n"
	@printf "  make record       Run NFM recorder\n"
	@printf "  make debug-idle   Measure idle RF/audio levels\n"
	@printf "  make debug-active Measure active RF/audio levels\n"
	@printf "  make debug-json   Emit debug stats as JSON\n"
	@printf "  make check        Validate Python entrypoints\n"

record:
	$(UV) run nfm-record

debug-idle:
	$(UV) run rf-power-debug --duration 3 --window-ms 250 --label idle

debug-active:
	$(UV) run rf-power-debug --duration 3 --window-ms 250 --label active

debug-json:
	$(UV) run rf-power-debug --duration 3 --window-ms 250 --label run --json

check:
	$(UV) run python -m py_compile nfm.py rf_debug.py
	$(UV) run nfm-record --help
	$(UV) run rf-power-debug --help
