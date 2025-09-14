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
from utils.helpers import group_variants, clean_value
from utils.prepare import prepare_product_data, prepare_variants_data
from config.settings import MYSQL_CONFIG
import mysql.connector  # type: ignore
import datetime as _dt
from pathlib import Path
import importlib


BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "web" / "uploads"
TEMPLATES_DIR = BASE_DIR / "web" / "templates"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


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
