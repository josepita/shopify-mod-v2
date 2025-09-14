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

