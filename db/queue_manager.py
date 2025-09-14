from __future__ import annotations

"""
Acceso a colas de actualización de precio/stock e históricos.
Usa mysql-connector como el resto de la capa DB actual.
"""
from typing import List, Dict, Tuple
import mysql.connector  # type: ignore
from config.settings import MYSQL_CONFIG


def _get_connection():
    return mysql.connector.connect(
        host=MYSQL_CONFIG.get("host"),
        user=MYSQL_CONFIG.get("user"),
        password=MYSQL_CONFIG.get("password"),
        database=MYSQL_CONFIG.get("database"),
        port=MYSQL_CONFIG.get("port", 3306),
    )


def get_queue_counts() -> Dict[str, int]:
    cnx = _get_connection()
    cur = cnx.cursor()
    counts = {"prices_pending": 0, "stock_pending": 0}
    cur.execute("SELECT COUNT(*) FROM price_updates_queue WHERE status='pending'")
    counts["prices_pending"] = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM stock_updates_queue WHERE status='pending'")
    counts["stock_pending"] = int(cur.fetchone()[0])
    cur.close(); cnx.close()
    return counts


def list_pending_prices(limit: int = 50) -> List[Dict]:
    cnx = _get_connection()
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT q.id, q.variant_mapping_id, q.new_price, vm.internal_sku, vm.shopify_variant_id, vm.shopify_product_id
        FROM price_updates_queue q
        LEFT JOIN variant_mappings vm ON vm.id = q.variant_mapping_id
        WHERE q.status = 'pending'
        ORDER BY q.created_at ASC
        LIMIT %s
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    cur.close(); cnx.close()
    return [
        {
            "id": r[0],
            "variant_mapping_id": r[1],
            "new_price": float(r[2] or 0),
            "sku": r[3],
            "shopify_variant_id": r[4],
            "shopify_product_id": r[5],
        }
        for r in rows
    ]


def list_pending_stock(limit: int = 50) -> List[Dict]:
    cnx = _get_connection()
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT q.id, q.variant_mapping_id, q.new_stock, vm.internal_sku, vm.shopify_variant_id, vm.shopify_product_id, vm.inventory_item_id
        FROM stock_updates_queue q
        LEFT JOIN variant_mappings vm ON vm.id = q.variant_mapping_id
        WHERE q.status = 'pending'
        ORDER BY q.created_at ASC
        LIMIT %s
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    cur.close(); cnx.close()
    return [
        {
            "id": r[0],
            "variant_mapping_id": r[1],
            "new_stock": int(r[2] or 0),
            "sku": r[3],
            "shopify_variant_id": r[4],
            "shopify_product_id": r[5],
            "inventory_item_id": r[6],
        }
        for r in rows
    ]


def mark_queue_status(table: str, item_id: int, status: str) -> None:
    cnx = _get_connection()
    cur = cnx.cursor()
    cur.execute(
        f"UPDATE {table} SET status=%s, processed_at=IF(%s IN ('completed','error'), CURRENT_TIMESTAMP, processed_at) WHERE id=%s",
        (status, status, int(item_id)),
    )
    cnx.commit()
    cur.close(); cnx.close()


def queue_changes_from_snapshots(process_type: str = 'all', limit: int | None = None) -> Dict[str, int]:
    """
    Detecta cambios entre el último snapshot y el anterior y llena colas.
    process_type: 'all' | 'prices' | 'stock'
    """
    cnx = _get_connection()
    cur = cnx.cursor()
    # Fechas
    cur.execute("SELECT MAX(snapshot_date) FROM catalog_snapshots")
    latest_row = cur.fetchone()
    latest = latest_row[0] if latest_row else None
    if not latest:
        cur.close(); cnx.close()
        return {"inserted_prices": 0, "inserted_stock": 0, "skipped_unmapped": 0}
    cur.execute("SELECT MAX(snapshot_date) FROM catalog_snapshots WHERE snapshot_date < %s", (latest,))
    prev_row = cur.fetchone()
    prev = prev_row[0] if prev_row else None
    if not prev:
        cur.close(); cnx.close()
        return {"inserted_prices": 0, "inserted_stock": 0, "skipped_unmapped": 0}

    inserted_prices = 0
    inserted_stock = 0
    skipped_unmapped = 0

    if process_type in ('all', 'prices'):
        # Detectar cambios de precio por referencia
        cur.execute(
            """
            SELECT vm.id, l.precio
            FROM catalog_snapshots l
            JOIN catalog_snapshots p ON p.reference = l.reference AND p.snapshot_date = %s
            JOIN variant_mappings vm ON vm.internal_sku = l.reference
            WHERE l.snapshot_date = %s
              AND l.precio IS NOT NULL AND p.precio IS NOT NULL
              AND l.precio <> p.precio
            """,
            (prev, latest),
        )
        changes = cur.fetchall()
        if limit:
            changes = changes[: int(limit)]
        for vm_id, new_price in changes:
            # Evitar duplicados pendientes
            cur.execute(
                "SELECT COUNT(*) FROM price_updates_queue WHERE variant_mapping_id=%s AND status='pending'",
                (vm_id,),
            )
            if int(cur.fetchone()[0]) == 0:
                cur.execute(
                    "INSERT INTO price_updates_queue (variant_mapping_id, new_price, status) VALUES (%s,%s,'pending')",
                    (vm_id, float(new_price)),
                )
                inserted_prices += 1

    if process_type in ('all', 'stock'):
        # Detectar cambios de stock por referencia
        cur.execute(
            """
            SELECT vm.id, l.stock
            FROM catalog_snapshots l
            JOIN catalog_snapshots p ON p.reference = l.reference AND p.snapshot_date = %s
            JOIN variant_mappings vm ON vm.internal_sku = l.reference
            WHERE l.snapshot_date = %s
              AND l.stock IS NOT NULL AND p.stock IS NOT NULL
              AND l.stock <> p.stock
            """,
            (prev, latest),
        )
        changes = cur.fetchall()
        if limit:
            changes = changes[: int(limit)]
        for vm_id, new_stock in changes:
            cur.execute(
                "SELECT COUNT(*) FROM stock_updates_queue WHERE variant_mapping_id=%s AND status='pending'",
                (vm_id,),
            )
            if int(cur.fetchone()[0]) == 0:
                cur.execute(
                    "INSERT INTO stock_updates_queue (variant_mapping_id, new_stock, status) VALUES (%s,%s,'pending')",
                    (vm_id, int(new_stock)),
                )
                inserted_stock += 1

    cnx.commit()
    cur.close(); cnx.close()
    return {
        "inserted_prices": inserted_prices,
        "inserted_stock": inserted_stock,
        "skipped_unmapped": skipped_unmapped,
    }
