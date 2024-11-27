"""
Script para crear y mantener la estructura de la base de datos
"""
import logging
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
    ]
    
    for table_sql in tables:
        print(f"Ejecutando migración...")
        cursor.execute(table_sql)
        print("Tabla creada exitosamente")

def run_migrations():
    """Ejecuta todas las migraciones necesarias"""
    print("Iniciando migraciones de base de datos...")
    connection = None
    
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
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