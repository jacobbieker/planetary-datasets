**Agents Guide**
- This file instructs automated coding agents working in this repository. It contains the reorganization plan (Dagster vs library), developer commands (build/lint/test) and coding style rules agents must follow.

**Repository context**
- Primary package: `planetary_datasets` (root package under `planetary_datasets/`).
- Provider implementations live in `planetary_datasets/providers/`, one module per dataset. Shared helpers are in `planetary_datasets/common/`, `planetary_datasets/config.py` and `planetary_datasets/memory.py`.
- There is a `dags/` directory used for workflows / assets. Keep Dagster code and job definitions in that area.

**High-level reorganization plan**
- Goal: split into two consumable parts: (1) a reusable library `planetary_datasets` with provider abstractions and utilities, and (2) a Dagster project containing pipelines/assets that use the library.
- Non-breaking approach: perform changes in small steps (create new folders, add adapters, update imports), run tests and linters between steps.

- Step 1 — Stabilize library surface
  - Providers live in the `planetary_datasets/providers` package, one `BaseProvider` subclass per dataset. The abstract base class is `BaseProvider` in `planetary_datasets/base.py`.
  - The contract is **not** `list`/`fetch`/`metadata`/`authenticate`. A subclass sets the `name`, `append_dim` and `store_prefix` attributes and implements two methods:
    - `fetch(it, temp_dir=None, **kwargs) -> List[str]` — return input URIs or local paths for one partition. An empty list means "nothing to do", not a failure.
    - `process(input_files, it, temp_dir=None, **kwargs) -> xr.Dataset` — turn those inputs into a dataset ready to write.
  - Callers (Dagster assets, the CLI) use `provider.run_partition(it) -> bool` for one partition and `provider.run_range(timestamps) -> int` for a range. `BaseProvider` handles opening the store, skipping partitions that are already written, memory guarding and the append-or-create write.
  - Credentials, bucket names and machine-specific paths come from `planetary_datasets.config.get_config()`; never hardcode them.
  - Shared I/O helpers (retrying downloads, precision reduction, icechunk writes) live in `planetary_datasets/common/`.

- Step 2 — Make library independent of Dagster
  - Ensure `planetary_datasets/__init__.py` exposes a stable public API and does not import Dagster.
  - Add `extras_require` in `setup.py` (or `pyproject.toml`) for `dagster` dependencies: `extras_require={'dagster': ['dagster', 'dagit']}`.

- Step 3 — Create Dagster project
  - Move existing workflow/asset code into `dags/` (or `dagster_project/`) keeping it separate from the library.
  - Dagster code should import the library `planetary_datasets` and call `run_partition` on the providers in `planetary_datasets.providers`.
  - Keep resource definitions, sensors and jobs inside the Dagster project folder.

- Step 4 — Migrate and test
  - Migrate providers incrementally, update imports, and run tests and linters.
  - Add tests for each provider verifying the `BaseProvider` contract (`fetch`, `process`, `run_partition`) against a local store.
  - Update CI to run library tests and Dagster tests separately.

**Build / Lint / Test commands**
- Create a virtualenv, install editable package for development:

```bash
# create venv (macOS/Linux)
python -m venv .venv
source .venv/bin/activate
# install project in editable mode
pip install -e .
# install dev tools (example)
pip install ruff black isort mypy pytest
```

- Run full test suite:

```bash
pytest -q
```

- Run a single test file:

```bash
pytest -q tests/path/to/test_file.py
```

- Run a single test function in a file:

```bash
pytest -q tests/path/to/test_file.py::test_function_name
```

- Run tests matching an expression (quick local iteration):

```bash
pytest -q -k "substring_of_test_name"
```

- Linting & formatting

```bash
# format files
black .
# unify import order
isort .
# lint / fast checks
ruff check .
# optionally auto-fix with ruff
ruff format .
```

- Static typing

```bash
mypy planetary_datasets
```

- Packaging

```bash
# build a wheel
python -m build
```

**If tests are missing**
- Add unit tests under `tests/` mirroring the `planetary_datasets/` layout. Prefer small, fast tests that stub `fetch` and exercise the real `process` against a local store (set `ICECHUNK_LOCAL_PATH`), rather than hitting the network.

**Code Style Guidelines (for agents)**
- Formatting: use `black` defaults and run `ruff format` before committing; keep lines to ~88-100 chars if possible.
- Linting: use `ruff` configured by `.ruff.toml` at repo root. Respect existing rules.
- Imports: use `isort` ordering: stdlib, third-party, first-party (project local), each group separated by a blank line.
  - Example:

```python
import os
import logging

import requests

from planetary_datasets.base import BaseProvider
```

- Type hints: prefer explicit typing for public functions and library surfaces. Use `-> None` for functions returning nothing and `Optional[...]` / `Union[...]` when appropriate. Run `mypy` to validate but keep strictness pragmatic — annotate public API first.
- Naming conventions:
  - Modules and functions: `snake_case` (e.g. `fetch_observations`).
  - Classes: `CapWords` (e.g. `GFSProvider`).
  - Constants: `UPPER_SNAKE_CASE`.
  - Private members: single leading underscore (e.g. `_download`).

- Exports & public API
  - Use `__all__` sparingly for modules intended to provide a curated surface. Prefer explicit re-exports in `planetary_datasets/__init__.py` for the library public API.

- Docstrings & comments
  - Use Google-style or NumPy-style docstrings consistently across the package. Include Args, Returns, Raises for public functions.
  - Keep comments short and factual. Do not add redundant comments that repeat the code.

- Error handling and logging
  - Avoid bare `except:`; catch specific exceptions.
  - For network I/O, use `planetary_datasets.common.download.download_one` / `download_many`, which already retry with exponential backoff and write atomically.
  - Raise domain-specific exceptions for provider errors rather than returning sentinel values. Configuration problems raise `planetary_datasets.config.MissingCredential`.
  - Use `loguru.logger` rather than prints; log at appropriate levels (DEBUG/INFO/WARNING/ERROR).

- Tests and flaky behavior
  - Write deterministic unit tests. For provider integration tests that require network access, mark them with `@pytest.mark.integration` and exclude by default in CI unless explicitly enabled.

- Dependency management
  - Keep core library dependencies minimal. Add optional dependencies for heavy-weight tools (e.g. `dagster`) under extras in `setup.py`.

**Repository hygiene**
- Avoid committing large data artifacts to Git. Move large data files into a `data/` directory and add them to `.gitignore`. Use object storage or separate release artifacts for large binaries.
- Keep virtualenvs and vendored site-packages out of the repository (`.venv`, `dags/assets/virt/` should not be committed).

**Automated agent rules**
- Agents must run `ruff format .` and `isort .` before proposing changes to code files, and must not alter unrelated files.
- When refactoring imports, update only the smallest possible set of files to keep CI green; prefer creating adapter shims in the old path that import from the new path.
- When making sweeping API changes, create a migration plan and run tests incrementally.

**Cursor / Copilot rules**
- No repository-level Cursor rules found in `.cursor/` or `.cursorrules`.
- No `.github/copilot-instructions.md` found. Follow the general agent rules in this file.

**Suggested next steps for agents working here**
1. Run `ruff check .` and `pytest -q` locally to get baseline failures.
2. Add one dataset at a time as a `BaseProvider` subclass under `planetary_datasets/providers/`, with a matching Dagster asset in `dags/assets/`, and ensure tests pass.
3. Repeat migration in small batches; open PRs that change at most one logical area per PR.

If you need to change CI or add pre-commit hooks, propose them as a separate PR referencing this guide.

End of AGENTS guide.
