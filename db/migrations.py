"""
Script para crear y mantener la estructura de la base de datos.

Incluye utilidades para comprobar compatibilidad del esquema actual
con el esperado por la aplicación, sin modificar datos.
"""
import logging
from typing import Dict, List, Tuple, Optional
import mysql.connector
from mysql.connector import Error
from config.settings import MYSQL_CONFIG

def create_tables(connection) -> None:
    """Crea las tablas en la base de datos"""
    cursor = connection.cursor()
    
    tables = [
        """
        CREATE TABLE IF NOT EXISTS product_mappings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            internal_reference VARCHAR(255) UNIQUE,
            shopify_product_id BIGINT,
            shopify_handle VARCHAR(255),
            title VARCHAR(255),
            first_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_internal_reference (internal_reference),
            INDEX idx_shopify_product_id (shopify_product_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS variant_mappings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            internal_sku VARCHAR(255) UNIQUE,
            shopify_variant_id BIGINT,
            shopify_product_id BIGINT,
            parent_reference VARCHAR(255),
            size VARCHAR(50),
            price DECIMAL(10,2),
            inventory_item_id BIGINT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_reference) 
                REFERENCES product_mappings(internal_reference)
                ON DELETE CASCADE,
            INDEX idx_internal_sku (internal_sku),
            INDEX idx_shopify_variant_id (shopify_variant_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """,
        """
        CREATE TABLE IF NOT EXISTS sync_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            internal_reference VARCHAR(255),
            action VARCHAR(50),
            status VARCHAR(50),
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_internal_reference (internal_reference),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS price_updates_queue (
            id INT AUTO_INCREMENT PRIMARY KEY,
            variant_mapping_id INT,
            new_price DECIMAL(10,2),
            status ENUM('pending','processing','completed','error') DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP NULL,
            INDEX idx_status (status),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS stock_updates_queue (
            id INT AUTO_INCREMENT PRIMARY KEY,
            variant_mapping_id INT,
            new_stock INT,
            status ENUM('pending','processing','completed','error') DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP NULL,
            INDEX idx_status (status),
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS price_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reference VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            date DATE NOT NULL,
            INDEX price_history_ref_date_idx (reference, date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS stock_history (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reference VARCHAR(255) NOT NULL,
            stock INT NOT NULL,
            date DATE NOT NULL,
            INDEX stock_history_ref_date_idx (reference, date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        ,
        """
        CREATE TABLE IF NOT EXISTS catalog_snapshots (
            id INT AUTO_INCREMENT PRIMARY KEY,
            snapshot_date DATETIME NOT NULL,
            reference VARCHAR(255) NOT NULL,
            base_reference VARCHAR(255) NOT NULL,
            descripcion VARCHAR(512) NULL,
            precio DECIMAL(10,2) NULL,
            stock INT NULL,
            categoria VARCHAR(255) NULL,
            subcategoria VARCHAR(255) NULL,
            tipo VARCHAR(255) NULL,
            imagen1 VARCHAR(1024) NULL,
            INDEX idx_snapshot_date (snapshot_date),
            INDEX idx_reference (reference),
            INDEX idx_base_reference (base_reference),
            INDEX idx_cat_subcat (categoria, subcategoria)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
    ]
    
    for table_sql in tables:
        print(f"Ejecutando migración...")
        cursor.execute(table_sql)
        print("Tabla creada exitosamente")


def _expected_schema() -> Dict[str, dict]:
    """Devuelve una representación simplificada del esquema esperado.

    Nota: Mantenemos esta estructura flexible para tolerar pequeñas diferencias
    (por ejemplo mayúsculas/minúsculas en tipos) y centrarnos en compatibilidad funcional.
    """
    return {
        "product_mappings": {
            "columns": {
                "id": {"type": "int", "extra": "auto_increment"},
                "internal_reference": {"type": "varchar(255)"},
                "shopify_product_id": {"type": "bigint"},
                "shopify_handle": {"type": "varchar(255)"},
                "title": {"type": "varchar(255)"},
                "first_created_at": {"type": "timestamp"},
                "last_updated_at": {"type": "timestamp"},
            },
            "primary_key": ["id"],
            "unique": [{"columns": ["internal_reference"]}],
            "indexes": [
                {"name": "idx_internal_reference", "columns": ["internal_reference"]},
                {"name": "idx_shopify_product_id", "columns": ["shopify_product_id"]},
            ],
        },
        "variant_mappings": {
            "columns": {
                "id": {"type": "int", "extra": "auto_increment"},
                "internal_sku": {"type": "varchar(255)"},
                "shopify_variant_id": {"type": "bigint"},
                "shopify_product_id": {"type": "bigint"},
                "parent_reference": {"type": "varchar(255)"},
                "size": {"type": "varchar(50)"},
                "price": {"type": "decimal(10,2)"},
                "inventory_item_id": {"type": "bigint"},
                "created_at": {"type": "timestamp"},
                "last_updated_at": {"type": "timestamp"},
            },
            "primary_key": ["id"],
            "unique": [{"columns": ["internal_sku"]}],
            "indexes": [
                {"name": "idx_internal_sku", "columns": ["internal_sku"]},
                {"name": "idx_shopify_variant_id", "columns": ["shopify_variant_id"]},
            ],
            "foreign_keys": [
                {
                    "column": "parent_reference",
                    "ref_table": "product_mappings",
                    "ref_column": "internal_reference",
                }
            ],
        },
        "sync_log": {
            "columns": {
                "id": {"type": "int", "extra": "auto_increment"},
                "internal_reference": {"type": "varchar(255)"},
                "action": {"type": "varchar(50)"},
                "status": {"type": "varchar(50)"},
                "message": {"type": "text"},
                "created_at": {"type": "timestamp"},
            },
            "primary_key": ["id"],
            "indexes": [
                {"name": "idx_internal_reference", "columns": ["internal_reference"]},
                {"name": "idx_created_at", "columns": ["created_at"]},
            ],
        },
        "price_updates_queue": {
            "columns": {
                "id": {"type": "int", "extra": "auto_increment"},
                "variant_mapping_id": {"type": "int"},
                "new_price": {"type": "decimal(10,2)"},
                "status": {"type": "enum"},
                "created_at": {"type": "timestamp"},
                "processed_at": {"type": "timestamp"},
            },
            "primary_key": ["id"],
            "indexes": [
                {"name": "idx_status", "columns": ["status"]},
                {"name": "idx_created_at", "columns": ["created_at"]},
            ],
        },
        "stock_updates_queue": {
            "columns": {
                "id": {"type": "int", "extra": "auto_increment"},
                "variant_mapping_id": {"type": "int"},
                "new_stock": {"type": "int"},
                "status": {"type": "enum"},
                "created_at": {"type": "timestamp"},
                "processed_at": {"type": "timestamp"},
            },
            "primary_key": ["id"],
            "indexes": [
                {"name": "idx_status", "columns": ["status"]},
                {"name": "idx_created_at", "columns": ["created_at"]},
            ],
        },
        "price_history": {
            "columns": {
                "id": {"type": "int", "extra": "auto_increment"},
                "reference": {"type": "varchar(255)"},
                "price": {"type": "decimal(10,2)"},
                "date": {"type": "date"},
            },
            "primary_key": ["id"],
            "indexes": [
                {"name": "price_history_ref_date_idx", "columns": ["reference", "date"]},
            ],
        },
        "stock_history": {
            "columns": {
                "id": {"type": "int", "extra": "auto_increment"},
                "reference": {"type": "varchar(255)"},
                "stock": {"type": "int"},
                "date": {"type": "date"},
            },
            "primary_key": ["id"],
            "indexes": [
                {"name": "stock_history_ref_date_idx", "columns": ["reference", "date"]},
            ],
        },
    }


def _fetch_columns(cursor, table: str) -> Dict[str, dict]:
    cursor.execute(
        """
        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """,
        (table,),
    )
    result = {}
    for name, col_type, is_nullable, default, extra in cursor.fetchall():
        result[name] = {
            "type": str(col_type or "").lower(),
            "nullable": (str(is_nullable or "").upper() == "YES"),
            "default": default,
            "extra": str(extra or "").lower(),
        }
    return result


def _fetch_primary_key(cursor, table: str) -> List[str]:
    cursor.execute(
        """
        SELECT k.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
          ON t.CONSTRAINT_NAME = k.CONSTRAINT_NAME
         AND t.TABLE_SCHEMA = k.TABLE_SCHEMA
         AND t.TABLE_NAME = k.TABLE_NAME
        WHERE t.TABLE_SCHEMA = DATABASE()
          AND t.TABLE_NAME = %s
          AND t.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY k.ORDINAL_POSITION
        """,
        (table,),
    )
    return [row[0] for row in cursor.fetchall()]


def _fetch_unique_constraints(cursor, table: str) -> List[List[str]]:
    cursor.execute(
        """
        SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as cols
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND NON_UNIQUE = 0 AND INDEX_NAME != 'PRIMARY'
        GROUP BY INDEX_NAME
        """,
        (table,),
    )
    uniques = []
    for _, cols in cursor.fetchall():
        uniques.append(cols.split(","))
    return uniques


def _fetch_indexes(cursor, table: str) -> Dict[str, List[str]]:
    cursor.execute(
        """
        SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        ORDER BY INDEX_NAME, SEQ_IN_INDEX
        """,
        (table,),
    )
    idx: Dict[str, List[str]] = {}
    for name, col, _ in cursor.fetchall():
        idx.setdefault(name, []).append(col)
    return idx


def _fetch_foreign_keys(cursor, table: str) -> List[dict]:
    cursor.execute(
        """
        SELECT k.COLUMN_NAME, k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
        WHERE k.TABLE_SCHEMA = DATABASE() AND k.TABLE_NAME = %s AND k.REFERENCED_TABLE_NAME IS NOT NULL
        """,
        (table,),
    )
    fks = []
    for col, ref_table, ref_col in cursor.fetchall():
        fks.append({"column": col, "ref_table": ref_table, "ref_column": ref_col})
    return fks


def check_schema_compatibility(connection) -> Tuple[bool, List[str]]:
    """Comprueba si el esquema actual es compatible con el esperado.

    Devuelve (es_compatible, lista_de_incidencias).
    """
    cursor = connection.cursor()
    issues: List[str] = []
    expected = _expected_schema()

    # Listar tablas existentes
    cursor.execute("SHOW TABLES")
    existing_tables = {row[0] for row in cursor.fetchall()}

    for table, spec in expected.items():
        if table not in existing_tables:
            issues.append(f"Tabla faltante: {table}")
            continue

        cols = _fetch_columns(cursor, table)
        pk = _fetch_primary_key(cursor, table)
        uniques = _fetch_unique_constraints(cursor, table)
        indexes = _fetch_indexes(cursor, table)
        fks = _fetch_foreign_keys(cursor, table)

        # Columnas requeridas
        for col_name, col_spec in spec.get("columns", {}).items():
            if col_name not in cols:
                issues.append(f"[{table}] Columna faltante: {col_name}")
                continue
            # Comparación de tipo (prefijo para permitir tamaños exactos)
            exp_type = col_spec.get("type", "").lower()
            got_type = cols[col_name]["type"].lower()
            if not got_type.startswith(exp_type):
                issues.append(
                    f"[{table}] Tipo incompatible en {col_name}: esperado '{exp_type}', actual '{got_type}'"
                )
            # AUTO_INCREMENT
            if col_spec.get("extra") == "auto_increment" and "auto_increment" not in cols[col_name]["extra"]:
                issues.append(f"[{table}] Falta AUTO_INCREMENT en columna {col_name}")

        # Primary key
        exp_pk = spec.get("primary_key") or []
        if exp_pk and pk != exp_pk:
            issues.append(f"[{table}] PRIMARY KEY diferente: esperado {exp_pk}, actual {pk}")

        # Uniques (basta con que exista al menos uno con las mismas columnas)
        for u in spec.get("unique", []):
            cols_set = u["columns"]
            if not any(uniq == cols_set for uniq in uniques):
                issues.append(f"[{table}] Falta UNIQUE en columnas {cols_set}")

        # Indexes (nombre puede variar; comprobamos columnas)
        existing_idx_cols = {tuple(v) for v in indexes.values()}
        for idx in spec.get("indexes", []):
            cols_tuple = tuple(idx["columns"]) 
            if cols_tuple not in existing_idx_cols:
                issues.append(f"[{table}] Falta índice en columnas {list(cols_tuple)}")

        # Foreign keys
        if spec.get("foreign_keys"):
            for fk in spec["foreign_keys"]:
                if not any(
                    f["column"].lower() == fk["column"].lower()
                    and f["ref_table"].lower() == fk["ref_table"].lower()
                    and f["ref_column"].lower() == fk["ref_column"].lower()
                    for f in fks
                ):
                    issues.append(
                        f"[{table}] Falta FOREIGN KEY {fk['column']} -> {fk['ref_table']}.{fk['ref_column']}"
                    )

    is_compatible = len(issues) == 0
    return is_compatible, issues


def apply_safe_upgrades(connection) -> List[str]:
    """Aplica mejoras no destructivas para compatibilizar esquema con esta app
    y con usos comunes del proyecto ../shopify-sync.

    Acciones que puede realizar:
    - Crear tabla sync_log si falta.
    - Añadir columna product_mappings.shopify_handle (NULLABLE) si falta.
    - Crear índices recomendados si faltan.
    NO cambia tipos de columnas ni añade restricciones FOREIGN KEY.

    Devuelve una lista de sentencias SQL ejecutadas.
    """
    executed: List[str] = []
    cursor = connection.cursor()

    # Asegurar tabla sync_log
    cursor.execute("SHOW TABLES")
    existing_tables = {row[0] for row in cursor.fetchall()}
    if "sync_log" not in existing_tables:
        create_sync_log_sql = (
            """
            CREATE TABLE IF NOT EXISTS sync_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                internal_reference VARCHAR(255),
                action VARCHAR(50),
                status VARCHAR(50),
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_internal_reference (internal_reference),
                INDEX idx_created_at (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cursor.execute(create_sync_log_sql)
        executed.append("CREATE TABLE sync_log")

    # Comprobar columnas e índices en product_mappings
    cols_pm = _fetch_columns(cursor, "product_mappings") if "product_mappings" in existing_tables else {}
    if "product_mappings" in existing_tables:
        # Columna shopify_handle (NULLABLE)
        if "shopify_handle" not in cols_pm:
            alter_sql = "ALTER TABLE product_mappings ADD COLUMN shopify_handle VARCHAR(255) NULL"
            cursor.execute(alter_sql)
            executed.append(alter_sql)

        # Índice sobre shopify_product_id
        idx_pm = _fetch_indexes(cursor, "product_mappings")
        idx_cols_pm = {tuple(v) for v in idx_pm.values()}
        if ("shopify_product_id",) not in idx_cols_pm:
            create_idx_sql = "CREATE INDEX idx_shopify_product_id ON product_mappings (shopify_product_id)"
            cursor.execute(create_idx_sql)
            executed.append(create_idx_sql)

    # Comprobar índices y columnas en variant_mappings
    if "variant_mappings" in existing_tables:
        # Columna inventory_item_id
        cols_vm = _fetch_columns(cursor, "variant_mappings")
        if "inventory_item_id" not in cols_vm:
            alter_vm = "ALTER TABLE variant_mappings ADD COLUMN inventory_item_id BIGINT NULL AFTER price"
            cursor.execute(alter_vm)
            executed.append(alter_vm)

        idx_vm = _fetch_indexes(cursor, "variant_mappings")
        idx_cols_vm = {tuple(v) for v in idx_vm.values()}
        if ("shopify_variant_id",) not in idx_cols_vm:
            create_idx_sql = "CREATE INDEX idx_shopify_variant_id ON variant_mappings (shopify_variant_id)"
            cursor.execute(create_idx_sql)
            executed.append(create_idx_sql)

    # Asegurar tablas de colas e históricos
    required_tables_sql = {
        "price_updates_queue": (
            """
            CREATE TABLE IF NOT EXISTS price_updates_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                variant_mapping_id INT,
                new_price DECIMAL(10,2),
                status ENUM('pending','processing','completed','error') DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP NULL,
                INDEX idx_status (status),
                INDEX idx_created_at (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ),
        "stock_updates_queue": (
            """
            CREATE TABLE IF NOT EXISTS stock_updates_queue (
                id INT AUTO_INCREMENT PRIMARY KEY,
                variant_mapping_id INT,
                new_stock INT,
                status ENUM('pending','processing','completed','error') DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP NULL,
                INDEX idx_status (status),
                INDEX idx_created_at (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ),
        "price_history": (
            """
            CREATE TABLE IF NOT EXISTS price_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                reference VARCHAR(255) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                date DATE NOT NULL,
                INDEX price_history_ref_date_idx (reference, date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ),
        "stock_history": (
            """
            CREATE TABLE IF NOT EXISTS stock_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                reference VARCHAR(255) NOT NULL,
                stock INT NOT NULL,
                date DATE NOT NULL,
                INDEX stock_history_ref_date_idx (reference, date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        ),
    }
    for tname, sql in required_tables_sql.items():
        if tname not in existing_tables:
            cursor.execute(sql)
            executed.append(f"CREATE TABLE {tname}")

    connection.commit()
    return executed

def run_migrations():
    """Ejecuta todas las migraciones necesarias.

    Antes de crear tablas ausentes, comprueba compatibilidad del esquema
    existente e informa de incidencias sin realizar cambios destructivos.
    """
    print("Iniciando migraciones de base de datos...")
    connection = None
    
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        # Comprobación previa de compatibilidad
        print("Comprobando compatibilidad de esquema...")
        compatible, issues = check_schema_compatibility(connection)
        if compatible:
            print("✔ Esquema compatible")
        else:
            print("⚠ Se detectaron diferencias de esquema:")
            for i, issue in enumerate(issues, 1):
                print(f"  {i}. {issue}")

        # Crear tablas que falten (no altera tablas existentes)
        create_tables(connection)
        connection.commit()
        print("Migraciones completadas exitosamente")
        
    except Error as e:
        print(f"Error durante las migraciones: {e}")
        if connection:
            connection.rollback()
        raise
    finally:
        if connection and connection.is_connected():
            connection.close()
            print("Conexión a MySQL cerrada")

if __name__ == "__main__":
    run_migrations()
