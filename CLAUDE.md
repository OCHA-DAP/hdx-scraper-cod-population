# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-cod-population** extracts data from COD (Common Operational Dataset) population statistics datasets on HDX to create a global dataset and a HAPI dataset. It makes several hundred read calls to HDX and two writes, and is run every weekday.

## Commands

Install dependencies:
```bash
uv sync
```

Run the scraper:
```bash
uv run python -m hdx.scraper.cod_population
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_cod_population.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

The pipeline in `__main__.py`:

1. **`main`** — Iterates over all country ISO3 codes, downloads COD population data for each, then generates and publishes a global dataset and a HAPI dataset to HDX.

### Key design points

- **Config files**: Static dataset metadata lives in `src/hdx/scraper/cod_population/config/`.
- **Saved data**: Downloaded data can be cached in `saved_data/` via `--save` / `--use-saved` flags to speed up re-runs during development.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-cod-population` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
