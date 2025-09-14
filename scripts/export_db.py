#!/usr/bin/env python3
"""
Exporta la base de datos MySQL a un archivo SQL portable.

Características:
- Carga configuración desde config/settings.py (no hardcodea credenciales).
- Exporta esquema (SHOW CREATE TABLE) y datos (INSERTs por lotes).
- Desactiva checks de FK durante la importación para mayor compatibilidad.
- Genera el volcado en backups/dump-YYYYmmdd-HHMMSS.sql

Uso:
  python scripts/export_db.py            # exporta toda la BD del .env
  python scripts/export_db.py --tables product_mappings,variant_mappings
  python scripts/export_db.py --output /ruta/archivo.sql
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import Iterable, List, Sequence

import mysql.connector  # type: ignore

from config.settings import MYSQL_CONFIG


def _escape(value) -> str:
    """Escapa valores para SQL INSERT (mínimo necesario)."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        # Manejar NaN/Inf
        if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
            return "NULL"
        return str(value)
    # Convertir a string y escapar
    s = str(value)
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _chunk(iterable: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def export_database(output_path: Path, only_tables: List[str] | None = None) -> Path:
    db_name = MYSQL_CONFIG.get("database")
    cnx = mysql.connector.connect(
        host=MYSQL_CONFIG.get("host"),
        user=MYSQL_CONFIG.get("user"),
        password=MYSQL_CONFIG.get("password"),
        database=db_name,
        port=MYSQL_CONFIG.get("port", 3306),
        autocommit=False,
    )
    cur = cnx.cursor()

    # Resolver tablas
    if only_tables:
        tables = only_tables
    else:
        cur.execute("SHOW TABLES")
        tables = [row[0] for row in cur.fetchall()]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"-- Dump generado: {now}\n")
        f.write(f"-- Base de datos: `{db_name}`\n\n")
        f.write("SET NAMES utf8mb4;\n")
        f.write("SET FOREIGN_KEY_CHECKS=0;\n\n")

        for table in tables:
            # Esquema
            cur.execute(f"SHOW CREATE TABLE `{table}`")
            row = cur.fetchone()
            create_sql = row[1] if row and len(row) > 1 else None
            if create_sql:
                f.write(f"--\n-- Estructura de tabla `{table}`\n--\n\n")
                f.write(f"DROP TABLE IF EXISTS `{table}`;\n")
                f.write(create_sql + ";\n\n")

            # Datos
            cur.execute(f"SELECT * FROM `{table}`")
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            if not rows:
                continue

            f.write(f"--\n-- Volcado de datos para la tabla `{table}`\n--\n\n")
            col_list = ", ".join(f"`{c}`" for c in columns)

            # INSERTs por lotes para evitar líneas enormes
            for batch in _chunk(rows, 500):
                values_sql = ",\n  ".join(
                    "(" + ", ".join(_escape(v) for v in row) + ")" for row in batch
                )
                f.write(f"INSERT INTO `{table}` ({col_list}) VALUES\n  {values_sql};\n")
            f.write("\n")

        f.write("SET FOREIGN_KEY_CHECKS=1;\n")

    cur.close()
    cnx.close()
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Exportar base de datos MySQL a SQL")
    parser.add_argument(
        "--tables",
        help="Lista de tablas separadas por comas a exportar (por defecto todas)",
    )
    parser.add_argument(
        "--output",
        help="Ruta del archivo de salida .sql (por defecto backups/dump-<timestamp>.sql)",
    )
    args = parser.parse_args()

    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    default_path = Path("backups") / f"dump-{ts}.sql"
    out = Path(args.output) if args.output else default_path

    only_tables = None
    if args.tables:
        only_tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    dump_path = export_database(out, only_tables)
    print(f"✅ Exportación completada: {dump_path}")


if __name__ == "__main__":
    main()

