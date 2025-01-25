import os
import json
import csv
import sys
import logging
from typing import Dict, List, Any
import mysql.connector
from mysql.connector import Error
import requests
import time
from datetime import datetime
from contextlib import contextmanager
import dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_stock_price.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
dotenv.load_dotenv()

MYSQL_CONFIG = {
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'host': os.getenv('MYSQL_HOST'),
    'port': os.getenv('MYSQL_PORT'),
}

SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_SHOP_URL = os.getenv('SHOPIFY_SHOP_URL')
SHOPIFY_LOCATION_ID = os.getenv('SHOPIFY_LOCATION_ID')

class ValidationError(Exception):
    pass

def validate_row(row: Dict[str, Any]) -> None:
    """Valida los datos de una fila del CSV"""
    try:
        price = float(row['PRECIO'])
        stock = int(row['STOCK'])
        
        if price <= 0:
            raise ValidationError(f"Precio inválido: {price}")
        if stock < 0:
            raise ValidationError(f"Stock inválido: {stock}")
    except (ValueError, KeyError) as e:
        raise ValidationError(f"Error en formato de datos: {e}")

def get_product_default_variant(product_id: str) -> Dict:
    """Obtiene la información de la variante por defecto de un producto simple"""
    query = {
        "query": """
        query getProductVariant($id: ID!) {
            product(id: $id) {
                variants(first: 1) {
                    edges {
                        node {
                            id
                            inventoryItem {
                                id
                            }
                        }
                    }
                }
            }
        }
        """,
        "variables": {
            "id": product_id
        }
    }

    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
    }

    response = requests.post(
        f"https://{SHOPIFY_SHOP_URL}/admin/api/2023-10/graphql.json",
        json=query,
        headers=headers,
        timeout=30
    )

    response_data = response.json()
    logger.debug(f"Respuesta de variante por defecto: {json.dumps(response_data, indent=2)}")

    if 'data' in response_data and response_data['data']['product']['variants']['edges']:
        variant = response_data['data']['product']['variants']['edges'][0]['node']
        return {
            'shopify_variant_id': variant['id'].split('/')[-1],
            'inventory_item_id': variant['inventoryItem']['id'].split('/')[-1]
        }
    else:
        raise Exception(f"No se pudo obtener la variante por defecto para el producto {product_id}")

def get_product_info(cursor, internal_reference: str) -> Dict:
    """Obtiene la información del producto o variante de las tablas de mapeo"""
    if '/' in internal_reference:
        # Caso para variantes
        parent_reference, size = internal_reference.split('/')
        cursor.execute(
            """
            SELECT shopify_variant_id, shopify_product_id, inventory_item_id
            FROM variant_mappings
            WHERE parent_reference = %s AND size = %s
            """,
            (parent_reference, size)
        )
        result = cursor.fetchone()
        if not result:
            raise Exception(f"Variante no encontrada: {internal_reference}")
    else:
        # Caso para productos simples
        cursor.execute(
            """
            SELECT shopify_product_id
            FROM product_mappings
            WHERE internal_reference = %s
            """,
            (internal_reference,)
        )
        result = cursor.fetchone()
        if not result:
            raise Exception(f"Producto no encontrado: {internal_reference}")
        
        # Obtener información de la variante por defecto
        product_id = f"gid://shopify/Product/{result['shopify_product_id']}"
        variant_info = get_product_default_variant(product_id)
        
        result = {
            'shopify_product_id': result['shopify_product_id'],
            'shopify_variant_id': variant_info['shopify_variant_id'],
            'inventory_item_id': variant_info['inventory_item_id']
        }

    return result

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def process_batch(batch: List[Dict], connection) -> None:
    """Procesa un lote de actualizaciones con reintentos"""
    cursor = connection.cursor(dictionary=True)
    start_time = time.time()
    
    try:
        # 1. Obtener toda la información necesaria de una vez
        for item in batch:
            product_info = get_product_info(cursor, item['internal_reference'])
            item.update({
                'variant_id': f"gid://shopify/ProductVariant/{product_info['shopify_variant_id']}" if product_info['shopify_variant_id'] else None,
                'product_id': f"gid://shopify/Product/{product_info['shopify_product_id']}",
                'inventory_item_id': product_info.get('inventory_item_id')
            })

        # 2. Agrupar una sola vez para precios
        variants_by_product = {}
        for item in batch:
            if item['product_id'] not in variants_by_product:
                variants_by_product[item['product_id']] = []
            variants_by_product[item['product_id']].append(item)

        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
        }

        # 3. Actualizar todos los precios
        price_update_errors = []
        price_start_time = time.time()
        for product_id, variants in variants_by_product.items():
            variants_input = [
                {
                    "id": item["variant_id"],
                    "price": str(item["price"])
                } for item in variants if item["variant_id"] is not None
            ]

            if not variants_input:
                continue

            price_mutation = {
                "query": """
                mutation updateVariants($productId: ID!, $input: [ProductVariantsBulkInput!]!) {
                    productVariantsBulkUpdate(productId: $productId, variants: $input) {
                        userErrors {
                            field
                            message
                        }
                    }
                }
                """,
                "variables": {
                    "productId": product_id,
                    "input": variants_input
                }
            }

            response = requests.post(
                f"https://{SHOPIFY_SHOP_URL}/admin/api/2023-10/graphql.json",
                json=price_mutation,
                headers=headers,
                timeout=30
            )
            response_data = response.json()
            if response.status_code != 200 or 'errors' in response_data:
                price_update_errors.append(response.json())

        price_end_time = time.time()
        print(f"\nTiempo actualización de precios: {price_end_time - price_start_time:.2f} segundos")

        if price_update_errors:
            logger.error(f"Errores actualizando precios: {json.dumps(price_update_errors, indent=2)}")
            raise Exception("Error actualizando precios")

        # 4. Actualizar inventario usando inventorySetQuantities
        inventory_start_time = time.time()
        # Construcción de la mutación para actualizar cantidades de inventario
        # Construcción del input para forzar cantidades
        set_quantities = [
            {
                "inventoryItemId": f"gid://shopify/InventoryItem/{item['inventory_item_id']}",
                "locationId": f"gid://shopify/Location/{SHOPIFY_LOCATION_ID}",
                "quantity": item["stock"],  # Cantidad que deseas establecer
                "compareQuantity": None  # Opcional, si no estás comparando con valores anteriores
            } for item in batch if item["variant_id"] is not None and item["inventory_item_id"]
        ]

        if set_quantities:
            inventory_mutation = {
                "query": """
                mutation inventorySetQuantities($input: InventorySetQuantitiesInput!) {
                inventorySetQuantities(input: $input) {
                    inventoryAdjustmentGroup {
                    reason
                    referenceDocumentUri
                    changes {
                        name
                        quantityAfterChange
                    }
                    }
                    userErrors {
                    code
                    field
                    message
                    }
                }
                }
                """,
                "variables": {
                    "input": {
                        "ignoreCompareQuantity": True,  # Ignorar cantidades previas
                        "name": "available",  # Nombre descriptivo
                        "reason": "correction",  # Razón de la corrección
                        "referenceDocumentUri": "logistics://some.warehouse/take/2024-12-24T08:38:39Z",  # Documento de referencia
                        "quantities": set_quantities  # Cantidades forzadas
                    }
                }
            }

            response = requests.post(
                f"https://{SHOPIFY_SHOP_URL}/admin/api/2024-07/graphql.json",
                json=inventory_mutation,
                headers=headers,
                timeout=30
            )

            response_data = response.json()
            print(f"Respuesta de actualización de inventario: {json.dumps(response_data, indent=2)}")

            if response.status_code != 200 or 'errors' in response_data:
                logger.error(f"Error actualizando inventario: {json.dumps(response_data, indent=2)}")
                raise Exception("Error actualizando inventario")

            if response_data.get('data', {}).get('inventorySetQuantities', {}).get('userErrors'):
                errors = response_data['data']['inventorySetQuantities']['userErrors']
                logger.error(f"Errores actualizando inventario: {errors}")
                raise Exception(f"Errores actualizando inventario: {errors}")


        inventory_end_time = time.time()
        print(f"Tiempo actualización de inventario: {inventory_end_time - inventory_start_time:.2f} segundos")

        # 5. Registrar en el log
        for item in batch:
            cursor.execute("""
                INSERT INTO price_stock_sync_log 
                (internal_reference, shopify_product_id, shopify_variant_id, 
                 action, old_price, new_price, old_stock, new_stock, 
                 status, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item["internal_reference"],
                item["product_id"].split("/")[-1],
                item["variant_id"].split("/")[-1] if item["variant_id"] else None,
                "update",
                None,
                item["price"],
                None,
                item.get("stock"),
                "success",
                "Updated successfully"
            ))
        
        connection.commit()
        total_time = time.time() - start_time
        print(f"\nTiempo total de procesamiento: {total_time:.2f} segundos")
        print(f"  - Tiempo actualización precios: {price_end_time - price_start_time:.2f} segundos")
        print(f"  - Tiempo actualización inventario: {inventory_end_time - inventory_start_time:.2f} segundos")
        logger.info(f"Lote de {len(batch)} items actualizado correctamente")

    except Exception as e:
        logger.error(f"Error procesando lote: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()

def update_stock_and_price(csv_file_path: str, batch_size: int = 100) -> None:
    """Actualiza el stock y precio en Shopify usando un archivo CSV"""
    start_time = time.time()
    
    with mysql.connector.connect(**MYSQL_CONFIG) as connection:
        logger.info("Conexión a la base de datos establecida")
        
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as csv_file:
            reader = csv.DictReader(csv_file, delimiter=';')
            reader.fieldnames = [header.strip() for header in reader.fieldnames]
            logger.info(f"Encabezados normalizados del CSV: {reader.fieldnames}")
            
            total_rows = sum(1 for _ in open(csv_file_path, encoding='utf-8-sig')) - 1
            processed = 0
            batch = []
            
            logger.info(f"Iniciando sincronización para {total_rows} registros")
            
            for row in reader:
                try:
                    validate_row(row)
                    internal_reference = row['REFERENCIA'].strip()
                    batch.append({
                        'internal_reference': internal_reference,
                        'price': float(row['PRECIO']),
                        'stock': int(row['STOCK'])
                    })
                    
                    if len(batch) >= batch_size:
                        process_batch(batch, connection)
                        batch = []
                    
                except ValidationError as e:
                    logger.error(f"Error de validación en fila {processed + 1}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error procesando {internal_reference}: {e}")
                    continue
                
                processed += 1
                if processed % 10 == 0:  # Mostrar progreso cada 10 registros
                    logger.info(f"Progreso: {processed}/{total_rows} registros procesados")
            
            if batch:
                process_batch(batch, connection)
    
    total_time = time.time() - start_time
    logger.info(f"Sincronización completada en {total_time:.2f} segundos")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Uso: python sync_stock_price.py <ruta_csv>")
        sys.exit(1)

    csv_file_path = sys.argv[1]
    try:
        update_stock_and_price(csv_file_path)
    except Exception as e:
        logger.error(f"Error en la sincronización: {e}")
        sys.exit(1)