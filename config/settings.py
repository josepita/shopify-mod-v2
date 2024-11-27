"""
Configuraciones globales del sistema de sincronización con Shopify
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración MySQL
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'database': os.getenv('MYSQL_DATABASE', 'shopify_sync'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'port': int(os.getenv('MYSQL_PORT', 3306))
}

# Configuración Shopify
SHOPIFY_ACCESS_TOKEN = os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_API_VERSION = os.getenv('SHOPIFY_API_VERSION', '2024-01')
SHOPIFY_SHOP_URL = os.getenv('SHOPIFY_SHOP_URL')

# Configuración de logging
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = os.path.join(LOG_DIR, 'shopify_sync.log')

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Silenciar logs de pyactiveresource
logging.getLogger('pyactiveresource.connection').setLevel(logging.WARNING)

# Validar configuración crítica
if not SHOPIFY_ACCESS_TOKEN:
    raise ValueError("SHOPIFY_ACCESS_TOKEN no está configurado en .env")

if not SHOPIFY_SHOP_URL:
    raise ValueError("SHOPIFY_SHOP_URL no está configurado en .env")

if not MYSQL_CONFIG['password']:
    raise ValueError("MYSQL_PASSWORD no está configurado en .env")

# Otras configuraciones
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 50))
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 30))