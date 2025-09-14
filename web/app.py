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
from config.settings import MYSQL_CONFIG
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


BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "web" / "uploads"
TEMPLATES_DIR = BASE_DIR / "web" / "templates"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
CATALOG_FILE = UPLOAD_DIR / "catalog-current"


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


def _fetch_mappings_for_refs(refs: list[str]):
    if not refs:
        return set(), {}
    cnx = mysql.connector.connect(
        host=MYSQL_CONFIG.get("host"),
        user=MYSQL_CONFIG.get("user"),
        password=MYSQL_CONFIG.get("password"),
        database=MYSQL_CONFIG.get("database"),
        port=MYSQL_CONFIG.get("port", 3306),
    )
    cur = cnx.cursor()
    placeholders = ",".join(["%s"] * len(refs))
    cur.execute(
        f"SELECT internal_reference FROM product_mappings WHERE internal_reference IN ({placeholders})",
        tuple(refs),
    )
    existing = {row[0] for row in cur.fetchall()}
    cur.execute(
        f"SELECT parent_reference, COUNT(*) FROM variant_mappings WHERE parent_reference IN ({placeholders}) GROUP BY parent_reference",
        tuple(refs),
    )
    variant_counts = {row[0]: int(row[1]) for row in cur.fetchall()}
    cur.close()
    cnx.close()
    return existing, variant_counts


def _build_catalog_records(df, q: str = "", categoria: str = "", subcategoria: str = "", estado: str = "todos"):
    grouped = group_variants(df)
    base_refs = [clean_value(ref) for ref in grouped.keys()]
    existing_set, variant_counts_map = _fetch_mappings_for_refs(base_refs)

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
        return {
            "referencia": clean_value(base_ref),
            "producto": product_data,
            "variantes": variants_data,
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
    context = {"request": request, "has_data": df is not None}
    if df is None:
        return templates.TemplateResponse("catalog.html", context)

    try:
        records, metrics, categorias, subcategorias = _build_catalog_records(df, q, categoria, subcategoria, estado)
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
        }
    )
    return templates.TemplateResponse("catalog.html", context)


@app.post("/catalog/upload", response_class=HTMLResponse)
def catalog_upload(request: Request, file: UploadFile = File(...)):
    filename = os.path.basename(file.filename)
    dest = _detect_catalog_path(filename)
    _save_upload(file, dest)
    return RedirectResponse(url="/catalog", status_code=303)


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
    return {
        "job_id": job.id,
        "status": job.status,
        "error": job.error_message,
        "logs": job.get_logs(tail=tail),
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
# Sección de COLAS
# =====================

@app.get("/queues", response_class=HTMLResponse)
def queues_page(request: Request, limit: int = 50):
    counts = qm.get_queue_counts()
    pending_prices = qm.list_pending_prices(limit=limit)
    pending_stock = qm.list_pending_stock(limit=limit)
    return templates.TemplateResponse(
        "queues.html",
        {
            "request": request,
            "counts": counts,
            "pending_prices": pending_prices,
            "pending_stock": pending_stock,
            "limit": limit,
        },
    )


def _process_price_queues(job: Job, batch_limit: int = 50, margin: float = 2.2):
    job.append_log("Iniciando procesamiento de cola de precios...\n")
    gql = ShopifyGraphQL()
    processed = 0
    items = qm.list_pending_prices(limit=batch_limit)
    for it in items:
        try:
            ok = gql.bulk_update_variant_price(str(it["shopify_product_id"]), str(it["shopify_variant_id"]), float(it["new_price"]), margin=margin)
            qm.mark_queue_status("price_updates_queue", it["id"], "completed" if ok else "error")
            processed += 1
            job.append_log(f"Precio SKU {it['sku']}: {'OK' if ok else 'ERROR'}\n")
        except Exception as e:
            qm.mark_queue_status("price_updates_queue", it["id"], "error")
            job.append_log(f"Error precio SKU {it['sku']}: {e}\n")
    job.append_log(f"Procesados precios: {processed}\n")


def _process_stock_queues(job: Job, batch_limit: int = 50):
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
            qm.mark_queue_status("stock_updates_queue", it["id"], "error")
            job.append_log(f"Error stock SKU {it['sku']}: {e}\n")
    job.append_log(f"Procesados stock: {processed}\n")


def _run_queue_job(job: Job, process_type: str = "all", batch_limit: int = 50):
    job.status = "running"
    try:
        if process_type in ("all", "prices"):
            _process_price_queues(job, batch_limit=batch_limit)
        if process_type in ("all", "stock"):
            _process_stock_queues(job, batch_limit=batch_limit)
        job.status = "done"
    except Exception as e:
        logging.exception("Error en procesamiento de colas")
        job.status = "error"
        job.error_message = str(e)


@app.post("/queues/process")
def queues_process(request: Request, type: str = Form("all"), batch: int = Form(50)):
    job = job_manager.create(filename=f"queues-{type}")
    thread = threading.Thread(target=_run_queue_job, args=(job, type, batch), daemon=True)
    thread.start()
    return RedirectResponse(url=f"/jobs/{job.id}", status_code=303)
