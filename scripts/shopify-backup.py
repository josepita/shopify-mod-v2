import os
import json
import time
import sys
from datetime import datetime
import requests
import logging
from typing import Dict, List, Any, Optional
import dotenv

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('shopify_backup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
dotenv.load_dotenv()

SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_SHOP_URL = os.getenv('SHOPIFY_SHOP_URL')

class ShopifyRateLimitError(Exception):
    """Excepción personalizada para errores de límite de rate"""
    pass

def get_products_query(cursor: str = None) -> str:
    """Genera la consulta GraphQL para obtener productos"""
    return """
    query getProducts($cursor: String) {
        products(first: 50, after: $cursor) {
            pageInfo {
                hasNextPage
                endCursor
            }
            edges {
                node {
                    id
                    title
                    handle
                    vendor
                    productType
                    createdAt
                    updatedAt
                    descriptionHtml
                    status
                    tags
                    priceRangeV2 {
                        minVariantPrice {
                            amount
                            currencyCode
                        }
                        maxVariantPrice {
                            amount
                            currencyCode
                        }
                    }
                    images(first: 10) {
                        edges {
                            node {
                                id
                                url
                                altText
                            }
                        }
                    }
                    variants(first: 100) {
                        edges {
                            node {
                                id
                                title
                                sku
                                price
                                compareAtPrice
                                inventoryQuantity
                                inventoryItem {
                                    id
                                    tracked
                                }
                                selectedOptions {
                                    name
                                    value
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """

def make_graphql_request(query: str, variables: Optional[Dict] = None, retry_count: int = 0) -> Dict:
    """
    Realiza una petición GraphQL a Shopify con manejo de límites de rate
    """
    max_retries = 3
    base_wait_time = 1.0  # Tiempo base de espera en segundos
    
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN
    }

    try:
        response = requests.post(
            f"https://{SHOPIFY_SHOP_URL}/admin/api/2023-04/graphql.json",
            headers=headers,
            json={"query": query, "variables": variables}
        )

        # Verificar límites de rate en los headers
        available_points = float(response.headers.get('X-Shopify-Shop-Api-Call-Limit', '0/40').split('/')[0])
        bucket_max = float(response.headers.get('X-Shopify-Shop-Api-Call-Limit', '0/40').split('/')[1])
        usage_ratio = available_points / bucket_max

        # Si estamos cerca del límite, esperar
        if usage_ratio > 0.75:  # Si hemos usado más del 75% de los puntos
            wait_time = base_wait_time * (2 ** retry_count)  # Backoff exponencial
            logger.warning(f"Acercándose al límite de rate ({usage_ratio*100:.1f}%). Esperando {wait_time} segundos...")
            time.sleep(wait_time)

        data = response.json()
        
        if 'errors' in data:
            if 'THROTTLED' in str(data['errors']):
                if retry_count < max_retries:
                    wait_time = base_wait_time * (2 ** retry_count)
                    logger.warning(f"Rate limit alcanzado. Esperando {wait_time} segundos antes de reintentar...")
                    time.sleep(wait_time)
                    return make_graphql_request(query, variables, retry_count + 1)
                else:
                    raise ShopifyRateLimitError("Máximo número de reintentos alcanzado")
            else:
                raise Exception(f"Error en la consulta GraphQL: {data['errors']}")

        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Error de red: {e}")
        raise

def fetch_all_products() -> List[Dict]:
    """Obtiene todos los productos de la tienda usando paginación y manejo de rate limits"""
    products = []
    has_next = True
    cursor = None
    
    while has_next:
        try:
            data = make_graphql_request(get_products_query(), {"cursor": cursor})
            
            page_info = data['data']['products']['pageInfo']
            product_edges = data['data']['products']['edges']
            
            products.extend([edge['node'] for edge in product_edges])
            
            has_next = page_info['hasNextPage']
            cursor = page_info['endCursor']
            
            logger.info(f"Obtenidos {len(product_edges)} productos. Total acumulado: {len(products)}")
            
            # Pausa base entre peticiones
            time.sleep(1)
            
        except ShopifyRateLimitError:
            logger.error("Se alcanzó el límite de rate de la API de Shopify")
            raise
        except Exception as e:
            logger.error(f"Error obteniendo productos: {e}")
            raise

    return products

def create_backup():
    """Crea una copia de seguridad del catálogo"""
    try:
        # Crear directorio de backup si no existe
        backup_dir = "shopify_backups"
        os.makedirs(backup_dir, exist_ok=True)
        
        # Generar nombre de archivo con timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{backup_dir}/shopify_catalog_backup_{timestamp}.json"
        
        # Obtener productos
        logger.info("Iniciando backup del catálogo...")
        products = fetch_all_products()
        
        # Guardar en archivo JSON
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                'backup_date': datetime.now().isoformat(),
                'shop_url': SHOPIFY_SHOP_URL,
                'total_products': len(products),
                'products': products
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Backup completado. Archivo guardado: {filename}")
        logger.info(f"Total de productos respaldados: {len(products)}")
        
        return filename
        
    except Exception as e:
        logger.error(f"Error creando backup: {e}")
        raise

if __name__ == "__main__":
    try:
        backup_file = create_backup()
        print(f"Backup completado exitosamente. Archivo: {backup_file}")
    except ShopifyRateLimitError:
        logger.error("El backup falló debido a límites de rate de la API")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error en el proceso de backup: {e}")
        sys.exit(1)