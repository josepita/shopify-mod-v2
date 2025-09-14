"""
Rellena el campo shopify_handle en product_mappings para registros
que ya tienen shopify_product_id pero no tienen handle.

Uso:
    python scripts/backfill_handles.py
"""
from __future__ import annotations

import time
import mysql.connector  # type: ignore
from config.settings import MYSQL_CONFIG


def setup_shopify():
    import importlib
    main_mod = importlib.import_module('main')
    if not main_mod.setup_shopify_api():
        raise RuntimeError('No se pudo conectar con Shopify (revisa .env)')


def iter_missing_rows(cur):
    cur.execute(
        "SELECT internal_reference, shopify_product_id FROM product_mappings "
        "WHERE (shopify_handle IS NULL OR shopify_handle='') AND shopify_product_id IS NOT NULL "
        "ORDER BY id ASC"
    )
    return cur.fetchall() or []


def run():
    cnx = mysql.connector.connect(
        host=MYSQL_CONFIG.get('host'),
        user=MYSQL_CONFIG.get('user'),
        password=MYSQL_CONFIG.get('password'),
        database=MYSQL_CONFIG.get('database'),
        port=MYSQL_CONFIG.get('port', 3306),
    )
    cur = cnx.cursor(dictionary=True)

    cur.execute(
        "SELECT COUNT(*) AS cnt FROM product_mappings WHERE (shopify_handle IS NULL OR shopify_handle='') AND shopify_product_id IS NOT NULL"
    )
    total = int(cur.fetchone()['cnt'])
    print(f"Pendientes de completar handle: {total}")

    setup_shopify()
    import shopify  # type: ignore

    processed = 0
    for row in iter_missing_rows(cur):
        ref = str(row['internal_reference']).strip()
        pid = int(row['shopify_product_id']) if row['shopify_product_id'] else None
        if not pid:
            continue
        try:
            prod = shopify.Product.find(pid)
            handle = getattr(prod, 'handle', None)
            if handle:
                up = cnx.cursor()
                up.execute(
                    "UPDATE product_mappings SET shopify_handle=%s, last_updated_at=CURRENT_TIMESTAMP WHERE internal_reference=%s",
                    (handle, ref),
                )
                cnx.commit()
                up.close()
                processed += 1
                print(f"{ref}: handle='{handle}' ✔")
            else:
                print(f"{ref}: sin handle (ID {pid})")
        except Exception as e:
            print(f"{ref}: error {e}")
        time.sleep(0.2)

    print(f"Completados: {processed} de {total}")


if __name__ == '__main__':
    run()

