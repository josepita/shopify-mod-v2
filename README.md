# Shopify Sync System

Sistema de sincronización y herramientas para gestionar catálogo y variantes en Shopify. Incluye:
- CLI para previsualizar/cargar datos desde Excel/CSV.
- UI web (FastAPI) para cargar archivos, explorar snapshots y comparar catálogos.
- Migraciones y utilidades MySQL.

## Requisitos
- Python 3.10+
- MySQL/MariaDB accesible (usuario/DB configurados en `.env`).

## Instalación rápida
1) Crear entorno virtual e instalar dependencias:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2) Configurar `.env` en la raíz del repo (ver `.env.example`). Variables mínimas:
```
SHOPIFY_ACCESS_TOKEN=...
SHOPIFY_SHOP_URL=tu-tienda.myshopify.com
MYSQL_HOST=127.0.0.1
MYSQL_DATABASE=shopify_sync
MYSQL_USER=usuario
MYSQL_PASSWORD=clave
SHOPIFY_API_VERSION=2024-01
```

3) Crear tablas en la base de datos:
```bash
python migrations_run.py
```

## Ejecutar la UI web
```bash
uvicorn web.app:app --reload --host 0.0.0.0 --port 8000
```
Abre `http://localhost:8000/` (docs: `http://localhost:8000/docs`).

Características relevantes de la UI:
- Carga de CSV/XLSX y archivado en `data/csv_archive/`.
- Snapshots en BD (`catalog_snapshots`) con métricas en “Catálogos guardados”.
- Comparación entre snapshots o archivos archivados, con difs en `data/catalog_diffs/`.

## Uso de la CLI
- Previsualizar en consola: `python main.py productos.xlsx screen-10`
- API CLI: `python main.py productos.xlsx api-50`
- Scripts: `python scripts/sync_stock_price.py` (y otros en `scripts/`).

## Desarrollo y pruebas
- Estilo: PEP8, tipado cuando sea posible, logs con `logging`.
- Tests (pytest) en `tests/`: `pytest -q`

### Documentación de metafields
- Referencia de metafields por tipo de producto: `docs/metafields.md`

## Dependencias clave
- FastAPI + Uvicorn.
- pandas, openpyxl/xlrd.
- mysql-connector-python.
- ShopifyAPI y cliente GraphQL propio (`services/shopify_graphql.py`).
- requests y beautifulsoup4 para descargas/parsing de catálogos remotos.

## Flags de rendimiento (opt-in, rollback inmediato)
Para optimizar el procesamiento de colas sin riesgo, se añadieron “feature flags” leídos desde `.env`. Todos están desactivados por defecto salvo la reutilización de sesión HTTP.

- `QUEUES_GROUP_PRICE_BY_PRODUCT` (default: `false`)
  - Agrupa elementos de la cola de precios por `shopify_product_id` y envía una sola mutación `productVariantsBulkUpdate` por producto (GraphQL). Reduce drásticamente el número de llamadas.

- `QUEUES_USE_GRAPHQL_STOCK_BULK` (default: `false`)
  - Usa `inventorySetQuantities` (Admin GraphQL) para actualizar stock en lote con una sola llamada, en lugar de `InventoryLevel.set` por ítem (REST).

- `QUEUES_DRAIN_CONTINUOUS` (default: `false`)
  - Hace que el worker de colas procese lotes sucesivos hasta vaciar o no hacer progreso, en lugar de un único lote.

- `SHOPIFY_GQL_USE_SESSION` (default: `true`)
  - Reutiliza conexión HTTP (`requests.Session`) para las llamadas GraphQL, reduciendo latencia por request.

- `QUEUES_ADAPTIVE_THROTTLE` (default: `false`)
  - Ajusta dinámicamente el tamaño del lote y pausas basándose en `extensions.cost.throttleStatus` de GraphQL (tokens disponibles, tope y ritmo de regeneración). Registra en logs métricas de throttle por lote.

Ejemplo de configuración en `.env` para una activación gradual:
```
# Reutilización de sesión (segura)
SHOPIFY_GQL_USE_SESSION=true

# Activar primero precios por grupos de producto
QUEUES_GROUP_PRICE_BY_PRODUCT=true

# Activar stock masivo cuando los mappings tengan inventory_item_id
QUEUES_USE_GRAPHQL_STOCK_BULK=true

# Drenado continuo opcional
QUEUES_DRAIN_CONTINUOUS=true

# Control adaptativo (opcional, beta)
QUEUES_ADAPTIVE_THROTTLE=true
```

Notas y recomendaciones:
- Comienza con `batch=50` desde la UI (`/queues`) y sube a 100–200 si el throttling lo permite.
- Si `inventory_item_id` falta en alguna variante, usa la UI para “reconciliar” mappings o deja que el worker resuelva por SKU (puede ser más lento).
- Si una llamada en lote da `userErrors`, el sistema marca cada elemento del lote con error y lo podrás reintentar.
- Con `QUEUES_ADAPTIVE_THROTTLE=true`, los logs del job (`/jobs/{id}`) incluyen líneas tipo:
  - `Throttle: cur=… max=… rate=… cost=… batch=…`
  - Y mensajes "Throttle bajo … Esperando Ns…" cuando aplica backoff.

Rollback:
- Deja todas las flags en `false` (salvo `SHOPIFY_GQL_USE_SESSION=true`) y reinicia el proceso; el comportamiento vuelve al actual sin cambios de esquema.

## Solución de problemas
- Snapshots no aparecen en la UI: ver ruta `/catalog/archive`. La función `_list_snapshot_stats` maneja `ONLY_FULL_GROUP_BY` con fallback; si no ves datos, valida en MySQL:
  ```sql
  SELECT COUNT(*) FROM catalog_snapshots;
  SELECT snapshot_date, COUNT(*) FROM catalog_snapshots GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 5;
  ```
- Variables `.env` faltantes: `config/settings.py` valida claves críticas y puede impedir el arranque.

## Estructura del proyecto (resumen)
- `config/` configuración y carga de `.env`.
- `db/` migraciones y acceso a MySQL.
- `services/` integraciones (GraphQL Shopify).
- `utils/` utilidades (parsing, agrupación, validación).
- `web/` app FastAPI, templates y rutas.
- `data/` archivos archivados y difs generados.
