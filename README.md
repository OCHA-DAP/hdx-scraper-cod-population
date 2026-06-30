# Collector for COD Population Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-cod-population/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-cod-population/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-cod-population/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-cod-population?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This script extracts data from the COD population statistics datasets on HDX to
create a global dataset and a HAPI dataset. It makes several hundred read calls
to HDX (approximately one per COD population dataset per country) and two writes.
Temporary per-country CSV files (up to a few MB each) are created during
processing and deleted afterwards. The two writes produce first a standard global
dataset (three CSV files split by admin level: `cod_population_admin0.csv`,
`cod_population_admin1.csv`, `cod_population_admin2.csv`, each up to a few MB),
and then a HAPI dataset generated from the same processed data. The input Excel
and CSV resources are parsed to extract gender- and age-disaggregated population
figures (using header patterns such as `F_0_5`, `M_65_plus`, `T_TL`); P-codes
are resolved against COD admin boundaries; encoding issues are normalised; and
output rows are enriched with HRP and GHO status before being written first to
the standard global dataset and then to the HAPI dataset. It runs every weekday
at around 10 AM UTC and takes approximately 15 minutes to complete.

## Data Pipeline

### API reads (several hundred calls per run)

- **COD population datasets** (~one HDX read per country): metadata and resource
  downloads for each country's COD population dataset. Each source file is an
  Excel or CSV containing gender- and age-disaggregated population counts
  (columns such as `F_0_5`, `M_65_plus`, `T_TL`) at admin levels 0–2.

### API writes (~2 calls per run)

- **Standard global dataset** (1 write): creates or updates a dataset with three
  CSV resources — `cod_population_admin0.csv`, `cod_population_admin1.csv`, and
  `cod_population_admin2.csv` — each up to a few MB.
- **HAPI dataset** (1 write): creates or updates the HAPI population dataset
  derived from the same processed data.

### Temporary files

- Per-country CSV files (up to a few MB each), created during processing and
  deleted afterwards.

### Uploaded files

- `cod_population_admin0.csv`, `cod_population_admin1.csv`,
  `cod_population_admin2.csv`: global CSVs split by admin level, each up to a
  few MB.
- HAPI dataset resources derived from the global output.

### Transformations

1. **Age/gender disaggregation**: column headers are parsed for patterns such as
   `F_0_5`, `M_65_plus`, and `T_TL` to extract sex and age-band values.
2. **P-code resolution**: admin codes in the source data are cross-referenced
   against COD admin boundaries to validate and canonicalise P-codes.
3. **Encoding normalisation**: character encoding issues in source files are
   detected and corrected.
4. **HRP/GHO enrichment**: each output row is annotated with `has_hrp` and
   `in_gho` flags looked up per ISO3 code.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key
 **hdx-scraper-cod_population** as specified in the parameter
 *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `EXTRA_PARAMS`, `TEMP_DIR`, and `LOG_FILE_ONLY`.

To run, execute:

```shell
    uv run python -m hdx.scraper.cod_population
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
