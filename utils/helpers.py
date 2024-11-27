"""
Funciones auxiliares para el procesamiento de datos y operaciones comunes
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
import re
import logging
from datetime import datetime

def clean_value(value: Any) -> str:
    """
    Limpia valores nulos y NaN, retornando string vacío en su lugar
    
    Args:
        value: Valor a limpiar
        
    Returns:
        str: Valor limpio o string vacío
    """
    if value is None or pd.isna(value) or value == 'nan' or value == 'NaN' or not str(value).strip():
        return ""
    return str(value).strip()

def is_variant_reference(reference: str) -> bool:
    """
    Determina si una referencia corresponde a una variante
    
    Args:
        reference (str): Referencia a verificar
        
    Returns:
        bool: True si es una referencia de variante
    """
    return '/' in reference

def get_base_reference(reference: str) -> str:
    """
    Obtiene la referencia base sin el número de talla
    
    Args:
        reference (str): Referencia completa
        
    Returns:
        str: Referencia base
    """
    return reference.split('/')[0] if '/' in reference else reference

def get_variant_size(reference: str) -> Optional[str]:
    """
    Extrae la talla de una referencia de variante
    
    Args:
        reference (str): Referencia completa
        
    Returns:
        Optional[str]: Talla extraída o None si no es una variante
    """
    return reference.split('/')[1] if '/' in reference else None

def format_price(price: Any) -> float:
    """
    Formatea un precio asegurando que sea un float válido
    
    Args:
        price: Precio en cualquier formato
        
    Returns:
        float: Precio formateado
    """
    try:
        if isinstance(price, str):
            # Eliminar caracteres no numéricos excepto punto y coma
            price = re.sub(r'[^\d.,]', '', price)
            # Reemplazar coma por punto
            price = price.replace(',', '.')
        return float(price)
    except (ValueError, TypeError):
        logging.warning(f"Error converting price: {price}")
        return 0.0

def validate_product_data(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Valida que un producto tenga todos los campos requeridos
    
    Args:
        data: Diccionario con datos del producto
        
    Returns:
        Tuple[bool, List[str]]: (es_válido, lista_de_errores)
    """
    required_fields = {
        'REFERENCIA': 'referencia',
        'DESCRIPCION': 'descripción',
        'PRECIO': 'precio',
        'TIPO': 'tipo'
    }
    
    missing_fields = []
    
    for field, name in required_fields.items():
        if field not in data or pd.isna(data[field]) or str(data[field]).strip() == '':
            missing_fields.append(name)
    
    return len(missing_fields) == 0, missing_fields

def group_variants(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Agrupa los productos y sus variantes por referencia base
    
    Args:
        df: DataFrame con los productos
        
    Returns:
        Dict[str, Dict]: Diccionario con productos agrupados
    """
    products = {}
    
    for _, row in df.iterrows():
        reference = clean_value(row['REFERENCIA'])
        base_reference = get_base_reference(reference)
        
        if base_reference not in products:
            products[base_reference] = {
                'is_variant_product': False,
                'base_data': row,
                'variants': []
            }
        
        if is_variant_reference(reference):
            products[base_reference]['is_variant_product'] = True
            products[base_reference]['variants'].append(row)
        elif len(products[base_reference]['variants']) == 0:
            products[base_reference]['variants'].append(row)
    
    return products

def format_title(reference: str, title: str) -> str:
    """
    Formatea el título del producto incluyendo la referencia base
    
    Args:
        reference (str): Referencia del producto
        title (str): Título original
        
    Returns:
        str: Título formateado
    """
    base_reference = get_base_reference(reference)
    if not isinstance(title, str):
        return base_reference
    
    formatted_title = re.sub(r'^(18K|9k)\s*', '', title)
    formatted_title = formatted_title.capitalize()
    
    return f"{formatted_title}"

def process_tags(category: str, subcategory: str, tipo: str) -> str:
    """
    Procesa y combina las etiquetas del producto
    
    Args:
        category (str): Categoría del producto
        subcategory (str): Subcategoría del producto
        tipo (str): Tipo de producto
        
    Returns:
        str: Etiquetas combinadas
    """
    tags = []
    
    for value in [clean_value(v) for v in [category, subcategory]]:
        if value:
            tags.append(value)

    tipo_clean = clean_value(tipo)
    if tipo_clean:
        tipo_norm = tipo_clean.strip().capitalize()
        if tipo_norm in ["Solitario", "Alianza", "Sello"]:
            tags.append(f"{tipo_norm}s")
    
    return ", ".join(filter(None, tags))

def log_processing_stats(start_time: datetime, processed: int, failed: int):
    """
    Registra estadísticas del procesamiento
    
    Args:
        start_time (datetime): Tiempo de inicio
        processed (int): Productos procesados
        failed (int): Productos fallidos
    """
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logging.info("="*40)
    logging.info("RESUMEN DE PROCESAMIENTO")
    logging.info("="*40)
    logging.info(f"Total productos procesados: {processed + failed}")
    logging.info(f"Productos exitosos: {processed}")
    logging.info(f"Productos con errores: {failed}")
    logging.info(f"Tiempo total: {duration:.2f} segundos")
    if processed > 0:
        logging.info(f"Tiempo promedio por producto: {(duration/processed):.2f} segundos")
    logging.info("="*40)

def format_log_message(product_ref: str, message: str, error: bool = False) -> str:
    """
    Formatea un mensaje de log
    
    Args:
        product_ref (str): Referencia del producto
        message (str): Mensaje
        error (bool): Si es un mensaje de error
        
    Returns:
        str: Mensaje formateado
    """
    prefix = "ERROR" if error else "INFO"
    return f"[{prefix}] [{product_ref}] {message}"

def extract_measures(description: str, product_type: str) -> dict:
   """
   Extrae medidas de la descripción del producto según reglas específicas por tipo.
   """
   def format_measure(value: str) -> str:
       """
       Formatea un número eliminando decimales si son .0
       Ejemplo: 12.0 -> 12, 12.5 -> 12.5
       """
       num = float(str(value).replace(',', '.'))
       return str(int(num)) if num.is_integer() else f"{num}"
   
   def normalize_number(value: str) -> str:
        """
        Normaliza un número convirtiendo comas en puntos
        """
        return str(float(str(value).replace(',', '.')))
   
   metafields = {}
   
   # Normalizar inputs
   description = description.lower().strip()
   product_type = product_type.lower().strip()
   
   # 1. Patrones de búsqueda
   patterns = {
       'medidas': r"(\d+(?:[.,]\d+)?)\s*x\s*(\d+(?:[.,]\d+)?)",  # Formato NxN
       'ancho': r"ancho\s+(\d+(?:[.,]\d+)?)\s*mm",             # Ancho específico
       'grosor': r"grosor\s+(\d+(?:[.,]\d+)?)\s*mm",           # Grosor específico
       'longitud_total': r"longitud\s+total\s+(\d+(?:[.,]\d+)?)\s*cm",
       'mm_generic': r"(\d+(?:[.,]\d+)?)\s*mm",                # Cualquier valor en mm
       'cm_generic': r"(\d+(?:[.,]\d+)?)\s*cm"                 # Cualquier valor en cm
   }
   
   # 2. PRIMERA PRIORIDAD: Buscar medidas en formato NxN
   medidas_match = re.search(patterns['medidas'], description)
   if medidas_match:
       if product_type == "sello":
           medida = f"{format_measure(medidas_match.group(1))}x{format_measure(medidas_match.group(2))}"
           if "grabado" in description[:description.find(f"{medidas_match.group(1)}x")]:
               metafields['medidas_zona_grabado'] = medida
           else:
               metafields['medidas_chaton'] = medida
       else:
           # Para cualquier otro tipo, primer número es alto y segundo ancho
           metafields['alto'] = normalize_number(medidas_match.group(1))
           metafields['ancho'] = normalize_number(medidas_match.group(2))
       return metafields
   
   # 3. SEGUNDA PRIORIDAD: longitud total
   if "longitud total" in description:
       match = re.search(patterns['longitud_total'], description)
       if match:
           metafields['largo'] = normalize_number(match.group(1))
           return metafields
   
   # 4. TERCERA PRIORIDAD: grosor o ancho explícito
   if "grosor" in description:
       match = re.search(patterns['grosor'], description)
       if match:
           metafields['grosor'] = normalize_number(match.group(1))
           
   if "ancho" in description:
       match = re.search(patterns['ancho'], description)
       if match:
           metafields['ancho'] = normalize_number(match.group(1))
   
   # 5. CUARTA PRIORIDAD: medidas genéricas
   # Procesar medidas en cm
   if 'largo' not in metafields:
       cm_matches = re.findall(patterns['cm_generic'], description)
       if cm_matches and product_type in ["esclava", "pulsera", "cadena", "collar"]:
           metafields['largo'] = normalize_number(cm_matches[0])
   
   # Procesar medidas en mm si no hay medidas anteriores
   mm_matches = re.findall(patterns['mm_generic'], description)
   
   if mm_matches:
       # Para aros y pendientes, la primera medida en mm sin especificar va a diámetro
       if product_type in ["aros", "pendientes"]:
           for mm_value in mm_matches:
               mm_value = normalize_number(mm_value)
               # Si el valor no coincide con un grosor o ancho ya registrado
               if ('grosor' not in metafields or metafields['grosor'] != mm_value) and \
                  ('ancho' not in metafields or metafields['ancho'] != mm_value):
                   metafields['diametro'] = mm_value
                   break
       
       # Para otros tipos, si solo hay una medida
       elif len(mm_matches) == 1 and not any(k in metafields for k in ['alto', 'ancho']):
           mm_value = normalize_number(mm_matches[0])
           if product_type in ["alianza", "solitario", "sortija"]:
               metafields['ancho'] = mm_value
           elif product_type in ["colgante", "medalla", "escapulario"]:
               metafields['diametro'] = mm_value
           elif product_type in ["esclava", "pulsera"]:
               metafields['grosor'] = mm_value
           elif product_type in ["cadena", "collar"]:
               metafields['ancho'] = mm_value
   
   return metafields

def extract_diamond_info(description: str) -> dict:
    """
    Extrae información sobre diamantes de la descripción del producto.
    
    Args:
        description: Descripción del producto
        
    Returns:
        dict: Diccionario con los metafields de diamantes
    """
    metafields = {}
    description = description.upper()
    
    # Solo procesar si contiene diamantes o brillantes
    if not any(word in description for word in ['BRILLANTE', 'DIAMANTE']):
        return metafields
        
    try:
        # Extraer quilates - patrón mejorado para incluir QT y QTS con espacios opcionales
        qts_pattern = r'(\d+[.,]\d+|\d+)\s*(?:QTS?|QT)\b'
        
        # Encontrar todas las coincidencias de quilates
        qts_matches = re.finditer(qts_pattern, description)
        last_qts = None
        
        # Procesar todas las coincidencias de quilates
        for match in qts_matches:
            last_qts = match.group(1).replace(',', '.')
            # Si hay "DIAMANTE" o "BRILLANTE" después de este número, es el que queremos
            pos_after_match = match.end()
            remaining_text = description[pos_after_match:pos_after_match + 30]
            if 'DIAMANTE' in remaining_text or 'BRILLANTE' in remaining_text:
                metafields['kilates_diamante'] = last_qts
                break
        
        # Si no encontramos quilates con contexto, usar el último encontrado
        if 'kilates_diamante' not in metafields and last_qts:
            metafields['kilates_diamante'] = last_qts

        # Patrones mejorados para color y pureza
        # 1. Patrón para "COLOR X"
        color_explicit_pattern = r'COLOR\s+([GHI])\b'
        # 2. Patrón para "PUREZA X"
        pureza_explicit_pattern = r'PUREZA\s+(FL|IF|WS|VVS1|VVS2|VS|VS1|VS2|SI|SI1|SI2|I1|I2|I3)\b'
        # 3. Patrón para combinaciones X-Y
        combined_pattern = r'([GHI])[-\s]?(FL|IF|WS|VVS1|VVS2|VS|VS1|VS2|SI|SI1|SI2|I1|I2|I3)|(FL|IF|WS|VVS1|VVS2|VS|VS1|VS2|SI|SI1|SI2|I1|I2|I3)[-\s]?([GHI])'

        # Buscar color explícito
        color_match = re.search(color_explicit_pattern, description)
        if color_match:
            metafields['color_diamante'] = color_match.group(1)

        # Buscar pureza explícita
        pureza_match = re.search(pureza_explicit_pattern, description)
        if pureza_match:
            metafields['calidad_diamante'] = pureza_match.group(1)

        # Si no se encontró alguno de los valores, buscar en el patrón combinado
        if not (color_match and pureza_match):
            combined_match = re.search(combined_pattern, description)
            if combined_match:
                # El color puede estar en el grupo 1 o 4
                color = combined_match.group(1) or combined_match.group(4)
                if color and 'color_diamante' not in metafields:
                    metafields['color_diamante'] = color
                
                # La calidad puede estar en el grupo 2 o 3
                calidad = combined_match.group(2) or combined_match.group(3)
                if calidad and 'calidad_diamante' not in metafields:
                    metafields['calidad_diamante'] = calidad
            
    except Exception as e:
        logging.error(f"Error extrayendo información de diamantes: {str(e)}")
        
    return metafields

def extract_stones(description: str) -> dict:
    """
    Extrae información sobre piedras del título/descripción del producto.
    
    Args:
        description: Descripción del producto
        
    Returns:
        dict: Diccionario con los metafields de piedras
    """
    metafields = {}
    description = description.lower()
    
    # Diccionario de piedras con sus variantes en plural
    stones = {
        'aguamarina': ['aguamarina', 'aguamarinas'],
        'alejandrita': ['alejandrita', 'alejandritas'],
        'amatista': ['amatista', 'amatistas'],
        'brillante': ['brillante', 'brillantes'],
        'circonita': ['circonita', 'circonitas'],
        'coral': ['coral', 'corales'],
        'cuarzo': ['cuarzo', 'cuarzos'],
        'diamante': ['diamante', 'diamantes'],
        'esmeralda': ['esmeralda', 'esmeraldas'],
        'granate': ['granate', 'granates'],
        'jade': ['jade', 'jades'],
        'perla': ['perla', 'perlas'],
        'topacio': ['topacio', 'topacios'],
        'turquesa': ['turquesa', 'turquesas'],
        'zafiro': ['zafiro', 'zafiros']
    }
    
    # Buscar piedras en la descripción
    found_stones = []
    for stone, variants in stones.items():
        if any(variant in description for variant in variants):
            found_stones.append(stone)
    
    # Si se encontraron piedras, añadirlas al metafield
    if found_stones:
        metafields['piedra'] = ', '.join(found_stones)
        
    return metafields