#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para ejecutar migraciones o comprobar el esquema.

Uso:
  - Ejecutar migraciones (crea tablas faltantes, no altera existentes):
      python migrations_run.py
  - Solo comprobar compatibilidad del esquema:
      python migrations_run.py --check
"""

import argparse
import mysql.connector
from db.migrations import run_migrations, check_schema_compatibility
from config.settings import MYSQL_CONFIG


def main() -> None:
    parser = argparse.ArgumentParser(description="Migraciones y verificación de esquema MySQL")
    parser.add_argument("--check", action="store_true", help="Solo comprobar compatibilidad del esquema")
    parser.add_argument("--upgrade-safe", action="store_true", help="Aplicar mejoras no destructivas (añadir columnas/índices/tabla sync_log)")
    args = parser.parse_args()

    if args.check:
        print("Verificando compatibilidad del esquema (no se harán cambios)...")
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        try:
            compatible, issues = check_schema_compatibility(conn)
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if compatible:
            print("✔ Esquema compatible con la aplicación")
        else:
            print("⚠ Diferencias detectadas:")
            for i, issue in enumerate(issues, 1):
                print(f"  {i}. {issue}")
        return

    if args.upgrade_safe:
        from db.migrations import apply_safe_upgrades, check_schema_compatibility
        print("Aplicando mejoras no destructivas del esquema...")
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        try:
            executed = apply_safe_upgrades(conn)
            if executed:
                print("Sentencias ejecutadas:")
                for sql in executed:
                    print(f"- {sql}")
            else:
                print("No hubo cambios: el esquema ya estaba alineado (safe upgrades)")
            # Informe post-upgrade
            compatible, issues = check_schema_compatibility(conn)
            if compatible:
                print("✔ Esquema compatible con la aplicación")
            else:
                print("⚠ Aún hay diferencias (no destructivas):")
                for i, issue in enumerate(issues, 1):
                    print(f"  {i}. {issue}")
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return

    run_migrations()


if __name__ == "__main__":
    main()
