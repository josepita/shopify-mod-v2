# Repository Guidelines

## Project Structure & Module Organization
- Source: `main.py` (entrypoint) and domain modules under `config/`, `db/`, `services/`, and `utils/`.
- Web UI: `web/` (FastAPI app) con `app.py`, plantillas en `web/templates/`, estáticos en `web/static/`, subidas en `web/uploads/`.
- Services: `services/shopify_graphql.py` (cliente GraphQL minimal para Shopify).
- Config: `config/settings.py` carga `.env` (Shopify + MySQL). Logs a `logs/`.
- Database: `db/migrations.py`, `db/mysql_connector.py`, `db/product_mapper.py`.
- Scripts: utilidades en `scripts/` (p.ej. `sync_stock_price.py`, `update_from_presta.py`).
- Datos: `data/` con `csv_archive/` y `catalog_diffs/`.
- Assets/docs: `reglas-medidas.md`, `tree.txt`. Requisitos en `requirements.txt`.

## Build, Test, and Development Commands
- Install: `pip install -r requirements.txt`
- DB setup: `python migrations_run.py` (crea las tablas MySQL requeridas).
- Run (CLI preview): `python main.py productos.xlsx screen-10`
- Run (CLI API): `python main.py productos.xlsx api-50`
- Run Web UI (FastAPI): `python -m uvicorn web.app:app --reload --host 0.0.0.0 --port 8000`
- Scripts: `python scripts/sync_stock_price.py` (sync de stock/precio), otros análogos.

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

## Notes for Agents
- Si el front no muestra snapshots en `/catalog/archive`, revisa `_list_snapshot_stats` en `web/app.py`. Existe un fallback que tolera `ONLY_FULL_GROUP_BY` y cursores dict.
- Requisitos web: además de FastAPI/Uvicorn, se usan `requests` y `beautifulsoup4` para descarga/parseo de catálogos remotos. Asegúrate de que estén listados en `requirements.txt`.
