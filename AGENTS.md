# Repository Guidelines

## Project Structure & Module Organization
- Source: `main.py` (entrypoint) and domain modules under `config/`, `db/`, and `utils/`.
- Config: `config/settings.py` loads `.env` (Shopify + MySQL). Logs write to `logs/`.
- Database: `db/migrations.py`, `db/mysql_connector.py`, `db/product_mapper.py`.
- Scripts: helper tools in `scripts/` (e.g., `sync_stock_price.py`, `update_from_presta.py`).
- Assets/docs: `reglas-medidas.md`, `tree.txt`. Requirements in `requirements.txt`.

## Build, Test, and Development Commands
- Install: `pip install -r requirements.txt`
- DB setup: `python migrations_run.py` (creates required MySQL tables).
- Run (preview): `python main.py productos.xlsx screen-10`
- Run (API): `python main.py productos.xlsx api-50`
- Scripts: `python scripts/sync_stock_price.py` (stock/price sync), others analogously.

## Coding Style & Naming Conventions
- Python 3.10+, 4‑space indentation, PEP 8; prefer type hints and docstrings.
- Use `snake_case` for functions/variables, `PascalCase` for classes, constants in `UPPER_SNAKE_CASE`.
- Keep domain terms and messages in Spanish (consistent with current codebase).
- Logging: prefer `logging` over `print` for persistent output; info/warn/error levels.
- Configuration must come from `config/settings.py` and `.env`—never hardcode secrets.

## Testing Guidelines
- Focus tests on `utils/` and `db/` behavior (e.g., parsing, grouping, SQL building).
- Use `pytest`; place tests under `tests/` with files like `tests/test_helpers.py`.
- Run locally: `pytest -q` (add `pytest` to a dev environment if not installed).
- Aim for coverage of critical flows: variant grouping, price formatting, MySQL interactions (use fakes/mocks).

## Commit & Pull Request Guidelines
- Commits: short, present tense, Spanish is fine. Example: `feat: soporte de variantes` or `fix: peso en gramos`.
- Reference issues when applicable (`Refs #123`). Group logical changes; avoid unrelated edits.
- PRs: include purpose, scope, before/after notes, and sample command/output. Link issues and add screenshots or logs when relevant.

## Security & Configuration Tips
- Create `.env` with: `SHOPIFY_ACCESS_TOKEN`, `SHOPIFY_SHOP_URL`, `MYSQL_HOST`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`, optional `SHOPIFY_API_VERSION`.
- Do not commit `.env`, credentials, or private data. Mask tokens in logs.
- Validate config at startup comes from `settings.py`; keep secrets out of code and tests.
