from __future__ import annotations

import io
import os
import threading
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .job_manager import job_manager, Job
from utils.helpers import group_variants, clean_value, get_base_reference
from utils.prepare import prepare_product_data, prepare_variants_data
from config.settings import MYSQL_CONFIG, CSV_URL, CSV_USERNAME, CSV_PASSWORD, PRICE_MARGIN, MAX_QUEUE_RETRIES, SHOPIFY_SHOP_URL
import mysql.connector  # type: ignore
import datetime as _dt
from pathlib import Path
import importlib
from fastapi import Query
from fastapi.responses import StreamingResponse
import csv
from db import queue_manager as qm
from services.shopify_graphql import ShopifyGraphQL
import logging
from utils.validator import validate_catalog_df
import requests
from bs4 import BeautifulSoup
import re
import threading as _threading
import time as _time
import time


BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "web" / "uploads"
TEMPLATES_DIR = BASE_DIR / "web" / "templates"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
CATALOG_FILE = UPLOAD_DIR / "catalog-current"
CATALOG_META = UPLOAD_DIR / "catalog-info.json"
ARCHIVE_DIR = BASE_DIR / "data" / "csv_archive"
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
ARCHIVE_DIR = BASE_DIR / "data" / "csv_archive"
ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)


app = FastAPI(title="Shopify Sync UI", version="0.1.0")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _save_upload(upload: UploadFile, dest_path: Path) -> None:
    contents = upload.file.read()
    dest_path.write_bytes(contents)


def _load_dataframe_for_preview(path: Path) -> Optional[object]:
    """Carga un DataFrame desde un archivo como en main.load_data, pero sin depender de settings."""
    # Importar pandas de forma diferida para evitar fallo en arranque si hay incompatibilidad
    try:
        import pandas as pd  # type: ignore
    except Exception as e:
        return None
    # Intentar como CSV
    try:
        encodings = ["utf-8", "latin1", "iso-8859-1"]
        for encoding in encodings:
            for sep in [",", ";", "\t"]:
                try:
                    df = pd.read_csv(path, encoding=encoding, sep=sep)
                    if len(df.columns) > 1:
                        df.columns = df.columns.str.strip()
                        return df
                except Exception:
                    continue
    except Exception:
        pass

    # Excel xlsx
    try:
        df = pd.read_excel(path, engine="openpyxl")
        df.columns = df.columns.str.strip()
        return df
    except Exception:
        pass

    # Excel xls
    try:
        df = pd.read_excel(path, engine="xlrd")
        df.columns = df.columns.str.strip()
        return df
    except Exception:
        pass

    return None


def _fetch_remote_catalog(url: str, username: str = "", password: str = "") -> Optional[object]:
    """Descarga un CSV directo o extrae la primera tabla HTML en un DataFrame."""
    try:
        import pandas as pd  # type: ignore
        auth = (username, password) if username and password else None
        resp = requests.get(url, auth=auth, timeout=60)
        resp.raise_for_status()
        content_type = resp.headers.get('Content-Type', '').lower()
        # CSV directo
        if 'text/csv' in content_type or url.lower().endswith('.csv'):
            from io import StringIO
            return pd.read_csv(StringIO(resp.text))
        # HTML con tabla
        if 'text/html' in content_type or '<table' in resp.text.lower():
            soup = BeautifulSoup(resp.text, 'html.parser')
            table = soup.find('table')
            if not table:
                return None
            # Extraer cabeceras de la primera fila
            rows = table.find_all('tr')
            headers = [td.get_text(strip=True) for td in rows[0].find_all(['td','th'])]
            data = []
            for tr in rows[1:]:
                cells = tr.find_all('td')
                if not cells:
                    continue
                row = {}
                for i, td in enumerate(cells):
                    if i < len(headers):
                        text = td.get_text(strip=True)
                        if headers[i] == 'PRECIO':
                            text = re.sub(r'[^\d.,]', '', text).replace(',', '.')
                        elif headers[i] == 'STOCK':
                            text = re.sub(r'[^\d]', '', text) or '0'
                        elif headers[i] == 'PESO G.':
                            text = re.sub(r'[^\d.,]', '', text).replace(',', '.')
                        row[headers[i]] = text
                if row:
                    data.append(row)
            df = pd.DataFrame(data)
            df.columns = df.columns.str.strip()
            return df
    except Exception:
        return None
    return None


def _filter_df_by_base_refs(df, base_refs: list[str]):
    try:
        import pandas as pd  # type: ignore
        base_set = {clean_value(x) for x in base_refs}
        if 'REFERENCIA' not in df.columns:
            return None
        tmp = df.copy()
        tmp['__BASE__'] = tmp['REFERENCIA'].apply(lambda v: get_base_reference(clean_value(v)))
        sub = tmp[tmp['__BASE__'].isin(base_set)].drop(columns=['__BASE__'])
        return sub
    except Exception:
        return None


def _reconcile_mappings_for_df(df_sub, job: Job):
    """Crea mapeos mínimos por SKU si faltan, consultando GraphQL por variante.
    Solo rellena product_mappings.shopify_product_id y variant_mappings básicos.
    """
    try:
        from db.product_mapper import ProductMapper
    except Exception:
        return
    mapper = ProductMapper(MYSQL_CONFIG)
    gql = ShopifyGraphQL()
    try:
        refs = df_sub['REFERENCIA'].dropna().astype(str).tolist()
    except Exception:
        return
    for sku in refs:
        sku = clean_value(sku)
        base = get_base_reference(sku)
        try:
            vm = mapper.get_variant_mapping(sku)
            if vm:
                continue
        except Exception:
            pass
        info = gql.get_variant_info_by_sku(sku)
        if not info or not info.get('variant_id') or not info.get('product_id'):
            continue
        pid = int(info['product_id'])
        vid = int(info['variant_id'])
        try:
            mapper.execute_query(
                """
                INSERT INTO product_mappings (internal_reference, shopify_product_id)
                VALUES (%s,%s)
                ON DUPLICATE KEY UPDATE shopify_product_id=VALUES(shopify_product_id), last_updated_at=CURRENT_TIMESTAMP
                """,
                (base, pid),
            )
            mapper.execute_query(
                """
                INSERT INTO variant_mappings (internal_sku, shopify_variant_id, shopify_product_id, parent_reference)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE shopify_variant_id=VALUES(shopify_variant_id), shopify_product_id=VALUES(shopify_product_id), last_updated_at=CURRENT_TIMESTAMP
                """,
                (sku, vid, pid, base),
            )
            job.append_log(f"Reconciliado mapping para {sku} (P:{pid} V:{vid})\n")
        except Exception as e:
            job.append_log(f"No se pudo reconciliar {sku}: {e}\n")


def _run_upload_selected_job(job: Job, base_refs: list[str], reconcile: bool, batch_limit: int = 0):
    job.status = 'running'
    try:
        df = _load_catalog_df()
        if df is None:
            raise RuntimeError('No hay catálogo cargado')
        df_sub = _filter_df_by_base_refs(df, base_refs)
        if df_sub is None or len(df_sub) == 0:
            raise RuntimeError('No se encontraron filas para las referencias seleccionadas')
        job.append_log(f"Filtradas {len(df_sub)} filas para {len(set(base_refs))} productos base\n")
        if reconcile:
            job.append_log("Reconciliando mapeos por SKU antes de subir…\n")
            _reconcile_mappings_for_df(df_sub, job)
        # Importar main y configurar API
        import importlib
        main_mod = importlib.import_module('main')
        if not main_mod.setup_shopify_api():
            raise RuntimeError('No se pudo conectar con Shopify (revisa .env)')
        # Procesar subset con logs redirigidos
        with redirect_stdout(_LogIO(job)), redirect_stderr(_LogIO(job)):
            main_mod.process_products(df=df_sub, display_mode=False)
        job.status = 'done'
    except Exception as e:
        job.status = 'error'
        job.error_message = str(e)
        job.append_log(f"ERROR: {e}")


def _load_catalog_df() -> Optional[object]:
    for ext in (".csv", ".xlsx", ".xls"):
        path = Path(str(CATALOG_FILE) + ext)
        if path.exists():
            return _load_dataframe_for_preview(path)
    return None


def _detect_catalog_path(filename: str) -> Path:
    lower = filename.lower()
    if lower.endswith(".csv"):
        return Path(str(CATALOG_FILE) + ".csv")
    if lower.endswith(".xlsx"):
        return Path(str(CATALOG_FILE) + ".xlsx")
    if lower.endswith(".xls"):
        return Path(str(CATALOG_FILE) + ".xls")
    return Path(str(CATALOG_FILE) + ".csv")


def _archive_and_snapshot_df(df, original_filename: str) -> None:
    ts = _dt.datetime.now()
    # Archivar CSV normalizado
    try:
        day_dir = ARCHIVE_DIR / ts.strftime("%Y%m%d")
        day_dir.mkdir(parents=True, exist_ok=True)
        safe_name = os.path.splitext(os.path.basename(original_filename))[0]
        out_path = day_dir / f"{safe_name}-{ts.strftime('%H%M%S')}.csv"
        df.to_csv(out_path, index=False)
    except Exception:
        pass

    # Volcar snapshot a BD
    try:
        cnx = mysql.connector.connect(
            host=MYSQL_CONFIG.get("host"),
            user=MYSQL_CONFIG.get("user"),
            password=MYSQL_CONFIG.get("password"),
            database=MYSQL_CONFIG.get("database"),
            port=MYSQL_CONFIG.get("port", 3306),
        )
        cur = cnx.cursor()
        cols = df.columns.str.strip().tolist()
        def gv(row, key, default=""):
            return str(row.get(key, default)) if key in row else default
        rows = []
        for _, r in df.iterrows():
            ref = str(r.get("REFERENCIA", "")).strip()
            if not ref:
                continue
            base_ref = get_base_reference(ref)
            try:
                precio = float(str(r.get("PRECIO", "0")).replace(",", ".")) if "PRECIO" in cols else None
            except Exception:
                precio = None
            try:
                stock = int(float(str(r.get("STOCK", "0")).replace(",", "."))) if "STOCK" in cols else None
            except Exception:
                stock = None
            rows.append(
                (
                    ts.strftime("%Y-%m-%d %H:%M:%S"),
                    ref,
                    base_ref,
                    gv(r, "DESCRIPCION"),
                    precio,
                    stock,
                    gv(r, "CATEGORIA"),
                    gv(r, "SUBCATEGORIA"),
                    gv(r, "TIPO"),
                    gv(r, "IMAGEN 1"),
                )
            )
        if rows:
            cur.executemany(
                """
                INSERT INTO catalog_snapshots
                (snapshot_date, reference, base_reference, descripcion, precio, stock, categoria, subcategoria, tipo, imagen1)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                rows,
            )
            cnx.commit()
        cur.close(); cnx.close()
    except Exception:
        pass


def _save_catalog_metadata(df, dest: Path, source: str) -> None:
    try:
        import json
        stats = dest.stat() if dest.exists() else None
        info = {
            "source": source,
            "path": str(dest.name),
            "saved_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rows": int(len(df)) if df is not None else None,
            "size_bytes": int(stats.st_size) if stats else None,
        }
        CATALOG_META.write_text(json.dumps(info), encoding="utf-8")
    except Exception:
        pass


def _fetch_mappings_for_refs(refs: list[str]):
    if not refs:
        return set(), {}, {}
    cnx = mysql.connector.connect(
        host=MYSQL_CONFIG.get("host"),
        user=MYSQL_CONFIG.get("user"),
        password=MYSQL_CONFIG.get("password"),
        database=MYSQL_CONFIG.get("database"),
        port=MYSQL_CONFIG.get("port", 3306),
    )
    cur = cnx.cursor()
    placeholders = ",".join(["%s"] * len(refs))
    # Traer también handle e ID para construir URLs
    cur.execute(
        f"SELECT internal_reference, shopify_handle, shopify_product_id FROM product_mappings WHERE internal_reference IN ({placeholders})",
        tuple(refs),
    )
    existing_rows = cur.fetchall()
    existing = {row[0] for row in existing_rows}
    # Preparar base de tienda
    shop_url = (SHOPIFY_SHOP_URL or '').strip().rstrip('/')
    if shop_url and not shop_url.startswith(('http://', 'https://')):
        shop_url = 'https://' + shop_url
    links_map: dict[str, dict] = {}
    for ref, handle, pid in existing_rows:
        try:
            handle = handle or ''
            pid = int(pid) if pid is not None else None
        except Exception:
            pid = None
        store_url = f"{shop_url}/products/{handle}" if shop_url and handle else None
        admin_url = f"{shop_url}/admin/products/{pid}" if shop_url and pid else None
        links_map[str(ref)] = {"store_url": store_url, "admin_url": admin_url}
    cur.execute(
        f"SELECT parent_reference, COUNT(*) FROM variant_mappings WHERE parent_reference IN ({placeholders}) GROUP BY parent_reference",
        tuple(refs),
    )
    variant_counts = {row[0]: int(row[1]) for row in cur.fetchall()}
    cur.close()
    cnx.close()
    return existing, variant_counts, links_map


def _build_catalog_records(df, q: str = "", categoria: str = "", subcategoria: str = "", estado: str = "todos"):
    grouped = group_variants(df)
    base_refs = [clean_value(ref) for ref in grouped.keys()]
    existing_set, variant_counts_map, links_map = _fetch_mappings_for_refs(base_refs)

    records = []
    total_variants = 0
    for base_ref, info in grouped.items():
        row = info["base_data"]
        base_ref = clean_value(base_ref)
        descripcion = clean_value(row.get("DESCRIPCION", ""))
        cat = clean_value(row.get("CATEGORIA", ""))
        subcat = clean_value(row.get("SUBCATEGORIA", ""))
        tipo = clean_value(row.get("TIPO", ""))
        precio = clean_value(row.get("PRECIO", ""))
        stock = clean_value(row.get("STOCK", ""))
        vcount = len(info.get("variants", []))
        total_variants += vcount
        estado_item = "subido" if base_ref in existing_set else "pendiente"

        # URL imagen original (del CSV)
        imagen_url = clean_value(row.get("IMAGEN 1", ""))

        link_info = links_map.get(base_ref, {}) if base_ref else {}
        rec = {
            "referencia": base_ref,
            "descripcion": descripcion,
            "categoria": cat,
            "subcategoria": subcat,
            "tipo": tipo,
            "precio": precio,
            "stock": stock,
            "variantes": vcount,
            "variantes_subidas": variant_counts_map.get(base_ref, 0),
            "estado": estado_item,
            "imagen_url": imagen_url,
            "store_url": link_info.get("store_url"),
            "admin_url": link_info.get("admin_url"),
        }
        records.append(rec)

    # Filtros
    q_norm = q.strip().lower()
    def match_q(r):
        if not q_norm:
            return True
        return (
            q_norm in r["referencia"].lower()
            or q_norm in r["categoria"].lower()
            or q_norm in r["subcategoria"].lower()
            or q_norm in r["descripcion"].lower()
        )

    filtered = [r for r in records if match_q(r)]
    if categoria:
        filtered = [r for r in filtered if r["categoria"].lower() == categoria.strip().lower()]
    if subcategoria:
        filtered = [r for r in filtered if r["subcategoria"].lower() == subcategoria.strip().lower()]
    if estado in ("subido", "pendiente"):
        filtered = [r for r in filtered if r["estado"] == estado]

    # Métricas
    total_products = len(records)
    subidos = sum(1 for r in records if r["estado"] == "subido")
    pendientes = total_products - subidos
    variants_subidas = sum(r["variantes_subidas"] for r in records)
    metrics = {
        "total_rows": int(len(df)),
        "total_products": total_products,
        "total_variants": total_variants,
        "subidos": subidos,
        "pendientes": pendientes,
        "coverage": round(subidos / total_products * 100, 2) if total_products else 0.0,
        "variants_subidas": variants_subidas,
    }

    categorias = sorted({r["categoria"] for r in records if r["categoria"]})
    subcategorias = sorted({r["subcategoria"] for r in records if r["subcategoria"]})

    return filtered, metrics, categorias, subcategorias


def _compute_category_stats(records: list[dict]) -> list[dict]:
    """Calcula estadísticas por categoría a partir de la lista de registros."""
    stats: dict[str, dict] = {}
    for r in records:
        cat = r.get("categoria") or "(Sin categoría)"
        st = stats.setdefault(cat, {"categoria": cat, "total": 0, "subidos": 0, "pendientes": 0})
        st["total"] += 1
        if r.get("estado") == "subido":
            st["subidos"] += 1
        else:
            st["pendientes"] += 1
    # Calcular cobertura y ordenar por nombre
    out: list[dict] = []
    for cat, st in stats.items():
        total = st["total"] or 1
        cobertura = round(st["subidos"] / total * 100, 2)
        st["cobertura"] = cobertura
        out.append(st)
    # Orden por pendientes desc, luego categoría
    out.sort(key=lambda x: (-x["pendientes"], x["categoria"]))
    return out


@app.get("/catalog/preview/{base_ref}")
def catalog_preview(base_ref: str):
    df = _load_catalog_df()
    if df is None:
        return JSONResponse({"error": "No hay catálogo cargado"}, status_code=400)
    try:
        grouped = group_variants(df)
        if base_ref not in grouped:
            # Intentar match por limpieza básica
            key = next((k for k in grouped.keys() if clean_value(k) == clean_value(base_ref)), None)
            if key is None:
                return JSONResponse({"error": f"Referencia {base_ref} no encontrada"}, status_code=404)
            base_ref = key
        info = grouped[base_ref]
        row = info["base_data"]
        product_data = prepare_product_data(row, clean_value(base_ref))
        variants_data = prepare_variants_data(info.get("variants", [])) if info.get("is_variant_product", False) else []
        # ¿Existe mapeo en MySQL? para decidir Crear/Actualizar
        exists = False
        store_url = None
        admin_url = None
        try:
            from db.product_mapper import ProductMapper
            mapper = ProductMapper(MYSQL_CONFIG)
            mapping = mapper.get_product_mapping(clean_value(base_ref))
            exists = mapping is not None
            if exists and mapping and mapping.get('product'):
                prod = mapping['product']
                handle = prod.get('shopify_handle')
                pid = prod.get('shopify_product_id')
                shop_url = (SHOPIFY_SHOP_URL or '').strip().rstrip('/')
                if shop_url and not shop_url.startswith(('http://', 'https://')):
                    shop_url = 'https://' + shop_url
                if shop_url and handle:
                    store_url = f"{shop_url}/products/{handle}"
                try:
                    pid_int = int(pid) if pid is not None else None
                except Exception:
                    pid_int = None
                if shop_url and pid_int:
                    admin_url = f"{shop_url}/admin/products/{pid_int}"
        except Exception:
            exists = False
        # Asegurar que el volcado CSV sea JSON-safe (reemplazar NaN/inf por None)
        try:
            import pandas as pd  # type: ignore
            csv_safe = row.where(pd.notna(row), None).to_dict()
        except Exception:
            try:
                csv_safe = row.to_dict()  # type: ignore[attr-defined]
            except Exception:
                csv_safe = {}
        return {
            "referencia": clean_value(base_ref),
            "producto": product_data,
            "variantes": variants_data,
            "csv": csv_safe,
            "exists": exists,
            "action": ("update" if exists else "create"),
            "store_url": store_url,
            "admin_url": admin_url,
        }
    except Exception as e:
        return JSONResponse({"error": f"Error preparando preview: {e}"}, status_code=500)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


@app.get("/db", response_class=HTMLResponse)
def db_tools(request: Request):
    return templates.TemplateResponse(
        "db.html",
        {"request": request}
    )


@app.get("/catalog", response_class=HTMLResponse)
def catalog(
    request: Request,
    q: str = Query(""),
    categoria: str = Query(""),
    subcategoria: str = Query(""),
    estado: str = Query("todos"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=1000),
    sort_by: str = Query("referencia"),
    sort_dir: str = Query("asc"),  # asc|desc
):
    df = _load_catalog_df()
    # Mensaje de feedback
    msg = request.query_params.get('msg')
    context = {
        "request": request,
        "has_data": df is not None,
        "csv_url": CSV_URL,
        "csv_username": CSV_USERNAME,
        "csv_password": CSV_PASSWORD,
        "msg": msg,
    }
    # Cargar metadatos del último catálogo
    try:
        import json
        if CATALOG_META.exists():
            meta = json.loads(CATALOG_META.read_text(encoding="utf-8"))
            # Tamaño legible
            def _hs(n):
                try:
                    n = int(n)
                    for unit in ['B','KB','MB','GB']:
                        if n < 1024:
                            return f"{n} {unit}"
                        n //= 1024
                    return f"{n} TB"
                except Exception:
                    return ""
            meta["size_human"] = _hs(meta.get("size_bytes"))
            context["last_catalog"] = meta
    except Exception:
        pass
    if df is None:
        return templates.TemplateResponse("catalog.html", context)

    try:
        records, metrics, categorias, subcategorias = _build_catalog_records(df, q, categoria, subcategoria, estado)
        validation = validate_catalog_df(df)
    except Exception as e:
        context.update({"error": f"Error preparando catálogo: {e}"})
        return templates.TemplateResponse("catalog.html", context, status_code=500)

    # Ordenación
    sort_by = (sort_by or "").lower()
    sort_dir = (sort_dir or "asc").lower()
    reverse = sort_dir == "desc"
    def sort_key(r):
        if sort_by in ("precio", "stock", "variantes"):
            try:
                return float(str(r.get(sort_by, 0)).replace(",", "."))
            except Exception:
                return 0.0
        return str(r.get(sort_by, "")).lower()
    if sort_by:
        records.sort(key=sort_key, reverse=reverse)

    total = len(records)
    pages = max(1, (total + per_page - 1) // per_page)
    if page > pages:
        page = pages
    start = (page - 1) * per_page
    end = start + per_page
    records_page = records[start:end]

    # Estadísticas por categoría (del catálogo completo, no del paginado)
    cat_stats = _compute_category_stats(records)

    context.update(
        {
            "records": records_page,
            "metrics": metrics,
            "categorias": categorias,
            "subcategorias": subcategorias,
            "q": q,
            "categoria": categoria,
            "subcategoria": subcategoria,
            "estado": estado,
            "page": page,
            "per_page": per_page,
            "total": total,
            "pages": pages,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "validation": validation,
            "cat_stats": cat_stats,
        }
    )
    return templates.TemplateResponse("catalog.html", context)


@app.post("/catalog/upload", response_class=HTMLResponse)
def catalog_upload(request: Request, file: UploadFile = File(...)):
    filename = os.path.basename(file.filename)
    dest = _detect_catalog_path(filename)
    _save_upload(file, dest)
    # Archivar y snapshot si es legible
    try:
        df = _load_dataframe_for_preview(dest)
        if df is not None:
            _archive_and_snapshot_df(df, filename)
            _save_catalog_metadata(df, dest, source=f"upload:{filename}")
    except Exception:
        pass
    return RedirectResponse(url="/catalog?msg=upload_ok", status_code=303)


@app.post('/catalog/upload_selected')
def catalog_upload_selected(request: Request, selected: list[str] = Form(...), reconcile: bool = Form(True)):
    if not selected:
        return RedirectResponse(url='/catalog?msg=no_selection', status_code=303)
    job = job_manager.create(filename='upload-selected')
    thread = threading.Thread(target=_run_upload_selected_job, args=(job, selected, reconcile), daemon=True)
    thread.start()
    return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)


@app.get("/catalog/export")
def catalog_export(
    q: str = Query(""),
    categoria: str = Query(""),
    subcategoria: str = Query(""),
    estado: str = Query("pendiente"),
    selected: list[str] | None = Query(None),
):
    df = _load_catalog_df()
    if df is None:
        return JSONResponse({"error": "No hay catálogo cargado"}, status_code=400)
    # Determinar referencias base a exportar
    filtered_records, _, _, _ = _build_catalog_records(df, q, categoria, subcategoria, estado)
    if selected:
        base_refs = {clean_value(s) for s in selected}
    else:
        base_refs = {r["referencia"] for r in filtered_records}

    # Filtrar filas del CSV original cuyo base_reference esté en base_refs
    try:
        import pandas as pd  # type: ignore
    except Exception as e:
        return JSONResponse({"error": f"Dependencia pandas ausente: {e}"}, status_code=500)

    def in_selection(val: str) -> bool:
        return clean_value(get_base_reference(clean_value(str(val)))) in base_refs

    df_sel = df[df["REFERENCIA"].apply(in_selection)] if "REFERENCIA" in df.columns else df

    # Orden de columnas solicitado
    columns_order = [
        "REFERENCIA",
        "DESCRIPCION",
        "PRECIO",
        "STOCK",
        "CATEGORIA",
        "SUBCATEGORIA",
        "METAL",
        "COLOR ORO",
        "TIPO",
        "PESO G.",
        "PIEDRA",
        "CALIDAD PIEDRA",
        "MEDIDAS",
        "CIERRE",
        "TALLA",
        "GENERO",
        "IMAGEN 1",
        "IMAGEN 2",
        "IMAGEN 3",
    ]
    cols = [c for c in columns_order if c in df_sel.columns]
    # Stream CSV manteniendo orden de columnas
    def _iter_rows():
        output = csv.StringIO()
        writer = csv.writer(output)
        writer.writerow(cols)
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)
        for _, row in df_sel.iterrows():
            values = []
            for c in cols:
                v = row.get(c, "")
                # Evitar escribir NaN/None/Null: convertir a cadena vacía
                if pd.isna(v) or v is None:
                    values.append("")
                else:
                    values.append(str(v))
            writer.writerow(values)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    ts = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"catalogo-seleccion-{ts}.csv"
    return StreamingResponse(_iter_rows(), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={filename}"})


def _latest_snapshot_date(cur) -> Optional[str]:
    cur.execute("SELECT MAX(snapshot_date) FROM catalog_snapshots")
    row = cur.fetchone()
    return row[0].strftime("%Y-%m-%d %H:%M:%S") if row and row[0] else None


@app.get("/catalog/discontinued")
def catalog_discontinued(days: int = 3, categoria: str = "", subcategoria: str = ""):
    try:
        cnx = mysql.connector.connect(
            host=MYSQL_CONFIG.get("host"),
            user=MYSQL_CONFIG.get("user"),
            password=MYSQL_CONFIG.get("password"),
            database=MYSQL_CONFIG.get("database"),
            port=MYSQL_CONFIG.get("port", 3306),
        )
        cur = cnx.cursor()
        latest = _latest_snapshot_date(cur)
        if not latest:
            return {"items": [], "count": 0}
        params = []
        where_cat = ""
        if categoria:
            where_cat += " AND categoria = %s"; params.append(categoria)
        if subcategoria:
            where_cat += " AND subcategoria = %s"; params.append(subcategoria)
        # Conjunto actual
        cur.execute(
            f"SELECT DISTINCT base_reference FROM catalog_snapshots WHERE snapshot_date = %s {where_cat}",
            tuple([latest] + params),
        )
        current_bases = {r[0] for r in cur.fetchall()}
        # Conjunto previo (últimos N días)
        cur.execute(
            f"""
            SELECT DISTINCT base_reference FROM catalog_snapshots
            WHERE snapshot_date < %s AND snapshot_date >= DATE_SUB(%s, INTERVAL %s DAY) {where_cat}
            """,
            tuple([latest, latest, int(days)] + params),
        )
        prior_bases = {r[0] for r in cur.fetchall()}
        missing = sorted(list(prior_bases - current_bases))
        items = []
        if missing:
            placeholders = ",".join(["%s"] * len(missing))
            cur.execute(
                f"""
                SELECT s.base_reference, s.descripcion, s.categoria, s.subcategoria, s.tipo, s.imagen1, MAX(s.snapshot_date)
                FROM catalog_snapshots s
                WHERE s.base_reference IN ({placeholders})
                GROUP BY s.base_reference, s.descripcion, s.categoria, s.subcategoria, s.tipo, s.imagen1
                ORDER BY s.base_reference
                """,
                tuple(missing),
            )
            for r in cur.fetchall():
                items.append({
                    "base_reference": r[0],
                    "descripcion": r[1],
                    "categoria": r[2],
                    "subcategoria": r[3],
                    "tipo": r[4],
                    "imagen1": r[5],
                    "last_seen": r[6].strftime("%Y-%m-%d %H:%M:%S") if r[6] else "",
                })
        cur.close(); cnx.close()
        return {"items": items, "count": len(items), "latest": latest}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/preview", response_class=HTMLResponse)
def preview(request: Request, file: UploadFile = File(...), n: int = Form(10)):
    # Guardar temporalmente el archivo para reutilizar en un posible "run"
    filename = os.path.basename(file.filename)
    tmp_path = UPLOAD_DIR / f"preview-{filename}"
    _save_upload(file, tmp_path)

    df = _load_dataframe_for_preview(tmp_path)
    if df is None:
        return templates.TemplateResponse(
            "preview.html",
            {
                "request": request,
                "error": f"No se pudo cargar el archivo {filename}. Formato no soportado.",
                "filename": filename,
            },
            status_code=400,
        )

    df = df.head(n)
    try:
        grouped = group_variants(df)
    except Exception as e:
        return templates.TemplateResponse(
            "preview.html",
            {
                "request": request,
                "error": f"Error agrupando variantes: {e}",
                "filename": filename,
            },
            status_code=400,
        )

    # Hacer un resumen compacto y datos Shopify para la vista
    resumen = []
    for base_ref, info in grouped.items():
        base_ref = clean_value(base_ref)
        row = info["base_data"]

        # Preparar estructuras que se enviarían a Shopify (sin llamar a la API)
        try:
            product_data = prepare_product_data(row, base_ref)
        except Exception as e:
            product_data = {"error": f"Error preparando datos: {e}"}
        variants_data = []
        if info.get("is_variant_product", False):
            try:
                variants_data = prepare_variants_data(info.get("variants", []))
            except Exception as e:
                variants_data = [{"error": f"Error preparando variantes: {e}"}]

        resumen.append(
            {
                "referencia": base_ref,
                "descripcion": clean_value(row.get("DESCRIPCION", "")),
                "tipo": clean_value(row.get("TIPO", "")),
                "precio": clean_value(row.get("PRECIO", "")),
                "variantes": len(info.get("variants", [])),
                "es_variantes": info.get("is_variant_product", False),
                "shopify": {
                    "producto": product_data,
                    "variantes": variants_data,
                },
            }
        )

    return templates.TemplateResponse(
        "preview.html",
        {
            "request": request,
            "filename": filename,
            "n": n,
            "resumen": resumen,
        },
    )


@app.post("/catalog/fetch", response_class=HTMLResponse)
def catalog_fetch(request: Request, url: str = Form(...), username: str = Form(""), password: str = Form("")):
    df = _fetch_remote_catalog(url, username=username, password=password)
    if df is None:
        return templates.TemplateResponse(
            "catalog.html",
            {"request": request, "has_data": False, "error": "No se pudo descargar o parsear el catálogo remoto."},
            status_code=400,
        )
    # Guardar como catalog-current.csv y archivar/snapshot
    dest = Path(str(CATALOG_FILE) + ".csv")
    try:
        df.to_csv(dest, index=False)
        _archive_and_snapshot_df(df, os.path.basename(url))
        _save_catalog_metadata(df, dest, source=f"fetch:{url}")
    except Exception:
        pass
    return RedirectResponse(url="/catalog?msg=fetch_ok", status_code=303)


@app.post("/catalog/snapshot")
def catalog_snapshot_now():
    # Crea snapshot desde el catalog-current.* si existe
    for ext in (".csv", ".xlsx", ".xls"):
        dest = Path(str(CATALOG_FILE) + ext)
        if dest.exists():
            try:
                df = _load_dataframe_for_preview(dest)
                if df is not None:
                    _archive_and_snapshot_df(df, dest.name)
                    _save_catalog_metadata(df, dest, source=f"snapshot:{dest.name}")
                    return RedirectResponse(url="/catalog?msg=snap_ok", status_code=303)
            except Exception:
                pass
    return RedirectResponse(url="/catalog?msg=snap_err", status_code=303)


class _LogIO(io.TextIOBase):
    """Canal para redirigir stdout/stderr a los logs del job."""

    def __init__(self, job: Job):
        self.job = job

    def write(self, s: str) -> int:
        self.job.append_log(s)
        return len(s)

    def flush(self) -> None:
        return None


def _run_sync_job(job: Job, full_path: Path, n: int) -> None:
    job.status = "running"
    job.started_at = os.times().elapsed if hasattr(os, "times") else None

    # Redirigir stdout/err y añadir handler de logging para capturar progreso
    import logging

    logger_handler = logging.StreamHandler(_LogIO(job))
    logger_handler.setLevel(logging.INFO)
    root_logger = logging.getLogger()
    root_logger.addHandler(logger_handler)

    try:
        # Importar main de forma diferida para no exigir .env en preview/inicio
        import importlib

        main_mod = importlib.import_module("main")

        # Cargar datos y limitar
        df = main_mod.load_data(str(full_path))
        if df is None:
            raise ValueError("No se pudo cargar el archivo (formato no soportado)")
        if n:
            df = df.head(n)

        # Configurar API Shopify
        if not main_mod.setup_shopify_api():
            raise RuntimeError("No se pudo establecer conexión con Shopify. Revisa .env")

        # Ejecutar procesamiento real
        job.append_log("Iniciando procesamiento con API...\n")
        with redirect_stdout(_LogIO(job)), redirect_stderr(_LogIO(job)):
            main_mod.process_products(df=df, display_mode=False)

        job.status = "done"
    except Exception as e:
        job.status = "error"
        job.error_message = str(e)
        job.append_log(f"\nERROR: {e}\n")
    finally:
        if root_logger and logger_handler:
            try:
                root_logger.removeHandler(logger_handler)
            except Exception:
                pass
        job.finished_at = os.times().elapsed if hasattr(os, "times") else None


@app.post("/run")
def run_job(request: Request, file: UploadFile = File(...), n: int = Form(10)):
    # Guardar el archivo subido con un nombre único por job
    filename = os.path.basename(file.filename)
    job = job_manager.create(filename=filename)
    full_path = UPLOAD_DIR / f"{job.id}-{filename}"
    _save_upload(file, full_path)

    # Lanzar el trabajo en un hilo
    thread = threading.Thread(target=_run_sync_job, args=(job, full_path, n), daemon=True)
    thread.start()

    return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_page(request: Request, job_id: str):
    job = job_manager.get(job_id)
    if not job:
        return HTMLResponse(f"Trabajo {job_id} no encontrado", status_code=404)
    return templates.TemplateResponse(
        "job.html",
        {"request": request, "job_id": job.id, "status": job.status},
    )


@app.get("/jobs/{job_id}/status")
def job_status(job_id: str, tail: int = 200):
    job = job_manager.get(job_id)
    if not job:
        return JSONResponse({"error": "Trabajo no encontrado"}, status_code=404)
    # Calcular métricas derivadas
    percent = None
    if getattr(job, "total", 0):
        try:
            percent = (job.completed / job.total) * 100.0
        except Exception:
            percent = None
    elapsed = None
    try:
        if job.started_at:
            import time as _t
            elapsed = _t.time() - job.started_at
    except Exception:
        elapsed = None
    return {
        "job_id": job.id,
        "status": job.status,
        "error": job.error_message,
        "logs": job.get_logs(tail=tail),
        "total": getattr(job, "total", 0),
        "processed": getattr(job, "completed", 0),
        "percent": percent,
        "eta_seconds": getattr(job, "eta_seconds", None),
        "elapsed_seconds": elapsed,
    }


@app.post("/db/import", response_class=HTMLResponse)
def import_database(request: Request, file: UploadFile = File(...)):
    try:
        sql_bytes = file.file.read()
        sql_text = sql_bytes.decode("utf-8", errors="ignore")

        cnx = mysql.connector.connect(
            host=MYSQL_CONFIG.get("host"),
            user=MYSQL_CONFIG.get("user"),
            password=MYSQL_CONFIG.get("password"),
            database=MYSQL_CONFIG.get("database"),
            port=MYSQL_CONFIG.get("port", 3306),
            autocommit=False,
            allow_multi_statements=True,
        )
        cur = cnx.cursor()
        for _ in cur.execute(sql_text, multi=True):
            pass
        cnx.commit()
        cur.close()
        cnx.close()

        return templates.TemplateResponse(
            "db.html",
            {
                "request": request,
                "success": f"Importación completada desde {file.filename}",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "db.html",
            {
                "request": request,
                "error": f"Error importando base de datos: {e}",
            },
            status_code=500,
        )


@app.get("/db/export")
def export_database():
    try:
        # Reutilizar el exportador del script
        exporter = importlib.import_module("scripts.export_db")
        ts = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        out_path = BASE_DIR / "backups" / f"dump-{ts}.sql"
        path = exporter.export_database(out_path)
        return FileResponse(
            str(path),
            media_type="application/sql",
            filename=path.name,
        )
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# =====================
# Backfill de handles
# =====================

def _run_backfill_handles_job(job: Job):
    job.status = "running"
    job.started_at = time.time()
    job.append_log("Iniciando backfill de handles de Shopify...\n")
    # Preparar conexión a BD
    try:
        cnx = mysql.connector.connect(
            host=MYSQL_CONFIG.get("host"),
            user=MYSQL_CONFIG.get("user"),
            password=MYSQL_CONFIG.get("password"),
            database=MYSQL_CONFIG.get("database"),
            port=MYSQL_CONFIG.get("port", 3306),
        )
        cur = cnx.cursor(dictionary=True)
    except Exception as e:
        job.status = "error"
        job.error_message = f"Error conectando a MySQL: {e}"
        job.append_log(job.error_message + "\n")
        return

    # Contar pendientes
    cur.execute(
        "SELECT COUNT(*) AS cnt FROM product_mappings WHERE (shopify_handle IS NULL OR shopify_handle='') AND shopify_product_id IS NOT NULL"
    )
    total = int(cur.fetchone()["cnt"])
    job.append_log(f"Pendientes de completar handle: {total}\n")
    job.set_progress(0, total, None)

    # Configurar API Shopify
    try:
        main_mod = importlib.import_module("main")
        if not main_mod.setup_shopify_api():
            raise RuntimeError("No se pudo establecer conexión con Shopify. Revisa .env")
    except Exception as e:
        job.status = "error"
        job.error_message = f"Error configurando API Shopify: {e}"
        job.append_log(job.error_message + "\n")
        cur.close(); cnx.close()
        return

    import shopify  # type: ignore
    processed = 0
    batch = 0
    try:
        cur.execute(
            "SELECT internal_reference, shopify_product_id FROM product_mappings WHERE (shopify_handle IS NULL OR shopify_handle='') AND shopify_product_id IS NOT NULL ORDER BY id ASC"
        )
        rows = cur.fetchall() or []
        for row in rows:
            ref = str(row["internal_reference"]).strip()
            pid = int(row["shopify_product_id"]) if row["shopify_product_id"] else None
            if not pid:
                continue
            try:
                prod = shopify.Product.find(pid)
                handle = getattr(prod, "handle", None)
                if handle:
                    up = cnx.cursor()
                    up.execute(
                        "UPDATE product_mappings SET shopify_handle=%s, last_updated_at=CURRENT_TIMESTAMP WHERE internal_reference=%s",
                        (handle, ref),
                    )
                    cnx.commit()
                    up.close()
                    processed += 1
                    batch += 1
                    job.append_log(f"{ref}: handle='{handle}' ✔\n")
                else:
                    job.append_log(f"{ref}: producto sin handle en Shopify (ID {pid})\n")
            except Exception as e:
                job.append_log(f"{ref}: error obteniendo producto {pid} -> {e}\n")
            # Respetar límites
            time.sleep(0.2)
            # Actualizar progreso + ETA periódicamente
            if batch >= 25:
                try:
                    elapsed = (time.time() - job.started_at) if job.started_at else None
                    rate = (processed / elapsed) if elapsed and elapsed > 0 else None
                    remaining = (total - processed)
                    eta = (remaining / rate) if rate else None
                except Exception:
                    eta = None
                job.set_progress(processed, total, eta)
                job.append_log(f"Progreso: {processed}/{total} ({(processed/total*100 if total else 0):.1f}%)\n")
                batch = 0
        job.append_log(f"Backfill finalizado. Completados: {processed} de {total}.\n")
        job.status = "done"
    except Exception as e:
        job.status = "error"
        job.error_message = str(e)
        job.append_log(f"ERROR backfill: {e}\n")
    finally:
        try:
            cur.close(); cnx.close()
        except Exception:
            pass


@app.post("/db/backfill_handles")
def backfill_handles_start():
    job = job_manager.create(filename="backfill-handles")
    thread = threading.Thread(target=_run_backfill_handles_job, args=(job,), daemon=True)
    thread.start()
    return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)


# =====================
# Sección de COLAS
# =====================

@app.get("/queues", response_class=HTMLResponse)
def queues_page(request: Request, limit: int = 50):
    try:
        counts = qm.get_queue_counts()
    except Exception:
        counts = {"prices_pending": 0, "stock_pending": 0}
    try:
        snap = qm.snapshot_stats()
    except Exception:
        snap = {"latest": None, "rows": 0}
    try:
        pending_prices = qm.list_pending_prices(limit=limit)
    except Exception:
        pending_prices = []
    try:
        pending_stock = qm.list_pending_stock(limit=limit)
    except Exception:
        pending_stock = []
    return templates.TemplateResponse(
        "queues.html",
        {
            "request": request,
            "counts": counts,
            "snapshot": snap,
            "pending_prices": pending_prices,
            "pending_stock": pending_stock,
            "limit": limit,
            "default_margin": PRICE_MARGIN,
            "max_retries": MAX_QUEUE_RETRIES,
        },
    )


def _process_price_queues(job: Job, batch_limit: int = 50, margin: float = PRICE_MARGIN) -> int:
    job.append_log("Iniciando procesamiento de cola de precios...\n")
    gql = ShopifyGraphQL()
    processed = 0
    items = qm.list_pending_prices(limit=batch_limit)
    for it in items:
        try:
            ok = gql.bulk_update_variant_price(str(it["shopify_product_id"]), str(it["shopify_variant_id"]), float(it["new_price"]), margin=margin)
            qm.mark_queue_status("price_updates_queue", it["id"], "completed" if ok else "processing")
            if not ok:
                qm.register_error("price_updates_queue", it["id"], "GraphQL update failed")
            else:
                qm.mark_queue_status("price_updates_queue", it["id"], "completed")
            processed += 1
            job.append_log(f"Precio SKU {it['sku']}: {'OK' if ok else 'ERROR'}\n")
        except Exception as e:
            qm.register_error("price_updates_queue", it["id"], str(e))
            job.append_log(f"Error precio SKU {it['sku']}: {e}\n")
    job.append_log(f"Procesados precios (lote): {processed}\n")
    return processed


def _process_stock_queues(job: Job, batch_limit: int = 50) -> int:
    job.append_log("Iniciando procesamiento de cola de stock...\n")
    # Reusar setup REST para niveles de inventario
    try:
        main_mod = importlib.import_module("main")
        if not main_mod.setup_shopify_api():
            raise RuntimeError("No se pudo establecer conexión con Shopify")
        location_id = main_mod.get_location_id()
    except Exception as e:
        job.append_log(f"Error configurando API Shopify: {e}\n")
        return

    import shopify  # type: ignore
    processed = 0
    items = qm.list_pending_stock(limit=batch_limit)
    for it in items:
        try:
            inv_item_id = it["inventory_item_id"]
            # Si faltara, intentar resolver por SKU vía GraphQL
            if not inv_item_id:
                gql = ShopifyGraphQL()
                info = gql.get_variant_info_by_sku(it["sku"])
                inv_item_id = info.get("inventory_item_id") if info else None
            if not inv_item_id:
                raise RuntimeError("No se pudo determinar inventory_item_id")
            shopify.InventoryLevel.set(location_id=location_id, inventory_item_id=inv_item_id, available=int(it["new_stock"]))
            qm.mark_queue_status("stock_updates_queue", it["id"], "completed")
            processed += 1
            job.append_log(f"Stock SKU {it['sku']}: OK\n")
        except Exception as e:
            qm.register_error("stock_updates_queue", it["id"], str(e))
            job.append_log(f"Error stock SKU {it['sku']}: {e}\n")
    job.append_log(f"Procesados stock (lote): {processed}\n")
    return processed


def _run_queue_job(job: Job, process_type: str = "all", batch_limit: int = 50, margin: float = PRICE_MARGIN):
    job.status = "running"
    try:
        if process_type in ("all", "prices"):
            _process_price_queues(job, batch_limit=batch_limit, margin=margin)
        if process_type in ("all", "stock"):
            _process_stock_queues(job, batch_limit=batch_limit)
        job.status = "done"
    except Exception as e:
        logging.exception("Error en procesamiento de colas")
        job.status = "error"
        job.error_message = str(e)


@app.post("/queues/process")
def queues_process(request: Request, type: str = Form("all"), batch: int = Form(50), margin: float = Form(None)):
    job = job_manager.create(filename=f"queues-{type}")
    m = float(margin) if margin is not None else PRICE_MARGIN
    thread = threading.Thread(target=_run_queue_job, args=(job, type, batch, m), daemon=True)
    thread.start()
    return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)


@app.post("/queues/process_json")
def queues_process_json(request: Request, type: str = Form("all"), batch: int = Form(50), margin: float = Form(None)):
    job = job_manager.create(filename=f"queues-{type}")
    m = float(margin) if margin is not None else PRICE_MARGIN
    thread = threading.Thread(target=_run_queue_job, args=(job, type, batch, m), daemon=True)
    thread.start()
    return {"job_id": job.id}


@app.get("/queues/stats")
def queues_stats():
    try:
        counts = qm.get_queue_counts()
    except Exception:
        counts = {"prices_pending": 0, "stock_pending": 0}
    try:
        snap = qm.snapshot_stats()
    except Exception:
        snap = {"latest": None, "rows": 0}
    return {"counts": counts, "snapshot": snap}


@app.post("/queues/retry_errors")
def queues_retry_errors():
    p = qm.retry_errors("price_updates_queue")
    s = qm.retry_errors("stock_updates_queue")
    return RedirectResponse(url="/queues", status_code=303)


@app.post("/queues/clear_errors")
def queues_clear_errors():
    p = qm.clear_errors("price_updates_queue")
    s = qm.clear_errors("stock_updates_queue")
    return RedirectResponse(url="/queues", status_code=303)


def _run_detect_job(job: Job, detect_type: str = "all", limit: int | None = None):
    job.status = "running"
    try:
        job.append_log("Detectando cambios entre snapshots y poblando colas...\n")
        snap = qm.snapshot_stats()
        job.append_log(f"Snapshot más reciente: {snap['latest']} — filas: {snap['rows']}\n")
        stats = qm.queue_changes_from_snapshots(process_type=detect_type, limit=limit or None)
        job.append_log(f"Insertados precios: +{stats['inserted_prices']}\n")
        job.append_log(f"Insertados stock: +{stats['inserted_stock']}\n")
        job.status = "done"
    except Exception as e:
        job.status = "error"
        job.error_message = str(e)


@app.post("/queues/detect")
def queues_detect(request: Request, type: str = Form("all"), limit: int = Form(0)):
    # Pre-chequeo: snapshot
    snap = qm.snapshot_stats()
    if not snap.get('latest'):
        return RedirectResponse(url="/catalog?msg=no_snapshot", status_code=303)
    job = job_manager.create(filename=f"detect-{type}")
    thread = threading.Thread(target=_run_detect_job, args=(job, type, (limit or None)), daemon=True)
    thread.start()
    return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)


def _run_force_job(job: Job, force_type: str = "all", limit: int | None = None):
    job.status = "running"
    try:
        job.append_log("Forzando llenado de colas desde snapshot actual...\n")
        snap = qm.snapshot_stats()
        job.append_log(f"Snapshot más reciente: {snap['latest']} — filas: {snap['rows']} (mapeadas: {snap['mapped']})\n")
        stats = qm.queue_force_from_snapshot(process_type=force_type, limit=limit or None)
        job.append_log(f"Insertados precios: +{stats['inserted_prices']}\n")
        job.append_log(f"Insertados stock: +{stats['inserted_stock']}\n")
        job.status = "done"
    except Exception as e:
        job.status = "error"
        job.error_message = str(e)


@app.post("/queues/force")
def queues_force(request: Request, type: str = Form("all"), limit: int = Form(0)):
    snap = qm.snapshot_stats()
    if not snap.get('latest'):
        return RedirectResponse(url="/catalog?msg=no_snapshot", status_code=303)
    job = job_manager.create(filename=f"force-{type}")
    thread = threading.Thread(target=_run_force_job, args=(job, type, (limit or None)), daemon=True)
    thread.start()
    return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)


# =====================
# Procesador continuo (arrancar/parar) con lotes
# =====================

class _ProcessorState:
    def __init__(self) -> None:
        self.running: bool = False
        self.process_type: str = "all"
        self.batch: int = 50
        self.margin: float = PRICE_MARGIN
        self.started_at: float | None = None
        self.last_tick_at: float | None = None
        self.proc_prices: int = 0
        self.proc_stock: int = 0
        self.job: Job | None = None
        self._stop = _threading.Event()
        self._thread: _threading.Thread | None = None
        self.total_goal: int = 0

    def to_dict(self) -> dict:
        # Calcular ETA/porcentaje con la info disponible
        try:
            counts = qm.get_queue_counts()
        except Exception:
            counts = {"prices_pending": 0, "stock_pending": 0}
        remaining = 0
        if self.process_type in ("all", "prices"):
            remaining += int(counts.get("prices_pending", 0))
        if self.process_type in ("all", "stock"):
            remaining += int(counts.get("stock_pending", 0))
        done = (self.total_goal - remaining) if self.total_goal else (self.proc_prices + self.proc_stock)
        elapsed = ( (_time.time() - (self.started_at or _time.time())) if self.running else 0 )
        rate = (done / elapsed) if (elapsed > 0 and done > 0) else 0
        eta_min = (remaining / rate / 60) if rate > 0 else None
        percent = (done / (done + remaining) * 100) if (done + remaining) > 0 else 0
        return {
            "running": self.running,
            "type": self.process_type,
            "batch": self.batch,
            "margin": self.margin,
            "started_at": self.started_at,
            "last_tick_at": self.last_tick_at,
            "processed_prices": self.proc_prices,
            "processed_stock": self.proc_stock,
            "job_id": self.job.id if self.job else None,
            "eta_min": eta_min,
            "percent": percent,
            "total_goal": self.total_goal,
            "remaining": remaining,
        }


processor = _ProcessorState()


def _processor_loop():
    ps = processor
    ps.running = True
    ps.started_at = _time.time()
    ps.proc_prices = 0
    ps.proc_stock = 0
    job = job_manager.create(filename=f"processor-{ps.process_type}")
    ps.job = job
    job.status = "running"
    job.append_log("Procesador iniciado. Ejecutando en lotes...\n")
    # Contadores iniciales y base para ETA
    try:
        q0 = qm.get_queue_counts()
    except Exception:
        q0 = {"prices_pending": 0, "stock_pending": 0}
    ps.total_goal = 0
    if ps.process_type in ("all", "prices"):
        ps.total_goal += int(q0.get("prices_pending", 0))
    if ps.process_type in ("all", "stock"):
        ps.total_goal += int(q0.get("stock_pending", 0))
    job.append_log(f"Pendientes iniciales — precios: {q0.get('prices_pending',0)}, stock: {q0.get('stock_pending',0)}, total: {ps.total_goal}\n")
    try:
        while not ps._stop.is_set():
            ps.last_tick_at = _time.time()
            # Si no hay snapshot, esperar
            snap = qm.snapshot_stats()
            if not snap.get("latest"):
                job.append_log("No hay snapshot. Esperando 5s...\n")
                _time.sleep(5)
                continue
            worked = 0
            if ps.process_type in ("all", "prices"):
                w = _process_price_queues(job, batch_limit=ps.batch, margin=ps.margin)
                ps.proc_prices += w
                worked += w
            if ps.process_type in ("all", "stock"):
                w2 = _process_stock_queues(job, batch_limit=ps.batch)
                ps.proc_stock += w2
                worked += w2
            # Log de avance + ETA
            try:
                q = qm.get_queue_counts()
                remaining = 0
                if ps.process_type in ("all", "prices"):
                    remaining += int(q.get("prices_pending", 0))
                if ps.process_type in ("all", "stock"):
                    remaining += int(q.get("stock_pending", 0))
                done = (ps.total_goal - remaining) if ps.total_goal else (ps.proc_prices + ps.proc_stock)
                elapsed = max(0.001, _time.time() - (ps.started_at or _time.time()))
                rate = done / elapsed if done > 0 else 0
                eta_sec = (remaining / rate) if rate > 0 else 0
                pct = (done / (done + remaining) * 100) if (done + remaining) > 0 else 0
                job.append_log(
                    f"Avance: done {done} / {done+remaining} ({pct:.1f}%) — ETA ~ {eta_sec/60:.1f} min"
                )
            except Exception:
                pass
            if worked == 0:
                # Nada que hacer, dormir un poco y reintentar
                _time.sleep(3)
            else:
                _time.sleep(0.2)
    except Exception as e:
        job.append_log(f"ERROR procesando: {e}\n")
        job.status = "error"
        job.error_message = str(e)
    finally:
        ps.running = False
        if job.status != "error":
            job.status = "done"


@app.post("/queues/processor/start")
def processor_start(type: str = Form("all"), batch: int = Form(50), margin: float = Form(None)):
    if processor.running:
        return JSONResponse({"error": "Ya está en ejecución", "status": processor.to_dict()}, status_code=409)
    processor.process_type = type
    processor.batch = int(batch)
    processor.margin = float(margin) if margin is not None else PRICE_MARGIN
    processor._stop.clear()
    t = _threading.Thread(target=_processor_loop, daemon=True)
    processor._thread = t
    t.start()
    return {"status": processor.to_dict()}


@app.post("/queues/processor/stop")
def processor_stop():
    processor._stop.set()
    return {"status": processor.to_dict()}


@app.get("/queues/processor/status")
def processor_status():
    return {"status": processor.to_dict()}
