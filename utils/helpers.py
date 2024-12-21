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

def process_tags(category: str, subcategory: str, tipo: str, description: str = "") -> str:
    """
    Procesa y combina las etiquetas del producto
    
    Args:
        category (str): Categoría del producto
        subcategory (str): Subcategoría del producto
        tipo (str): Tipo de producto
        description (str): Descripción del producto
        
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
            
    # Comprobar si hay símbolo del zodiaco
    description = description.lower()
    zodiac_signs = ['aries', 'tauro', 'geminis', 'cancer', 'leo', 'virgo', 
                    'libra', 'escorpio', 'sagitario', 'capricornio', 'acuario', 'piscis']
    
    if any(sign in description for sign in zodiac_signs):
        tags.append("Horoscopo")
    
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

def extract_zodiac_info(description: str) -> dict:
    """
    Extrae información sobre símbolos del zodiaco de la descripción.
    
    Args:
        description: Descripción del producto
        
    Returns:
        dict: Diccionario con los metafields del zodiaco
    """
    def normalize_text(text: str) -> str:
        """
        Normaliza el texto: elimina acentos y convierte a minúsculas
        """
        replacements = {
            'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
            'Á': 'a', 'É': 'e', 'Í': 'i', 'Ó': 'o', 'Ú': 'u'
        }
        text = text.lower()
        for orig, repl in replacements.items():
            text = text.replace(orig, repl)
        return text

    metafields = {}
    normalized_description = normalize_text(description)
    
    # Lista de símbolos del zodiaco con sus nombres normalizados y oficiales
    zodiac_signs = {
        'aries': 'Aries',
        'tauro': 'Tauro',
        'geminis': 'Géminis',
        'cancer': 'Cáncer',
        'leo': 'Leo',
        'virgo': 'Virgo',
        'libra': 'Libra',
        'escorpio': 'Escorpio',
        'sagitario': 'Sagitario',
        'capricornio': 'Capricornio',
        'acuario': 'Acuario',
        'piscis': 'Piscis'
    }
    
    # Buscar símbolos en la descripción normalizada
    for sign_key, sign_name in zodiac_signs.items():
        if sign_key in normalize_text(normalized_description):
            metafields['simbolo_zodiaco'] = sign_name
            break
            
    return metafields

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

    # Lista de tipos que pueden tener largo
    TIPOS_CON_LARGO = ["esclava", "pulsera", "cadena", "collar", "gargantilla", "cordon"]
   
    # 1. Patrones de búsqueda
    patterns = {
        'medidas': r"(\d+(?:[.,]\d+)?)\s*x\s*(\d+(?:[.,]\d+)?)|(\d+(?:[.,]\d+)?)\s+x\s+(\d+(?:[.,]\d+)?)",
        'ancho': r"ancho\s*:?\s*(\d+(?:[.,]\d+)?)\s*mm",
        'grosor': r"grosor\s*:?\s*(\d+(?:[.,]\d+)?)\s*mm",
        'alto': r"alto\s*:?\s*(\d+(?:[.,]\d+)?)\s*mm",
        'diametro': r"diametro\s*:?\s*(\d+(?:[.,]\d+)?)\s*mm",
        'largo': r"largo\s*:?\s*(\d+(?:[.,]\d+)?)\s*(?:mm|cm)",
        'longitud_total': r"longitud\s+total\s*:?\s*(\d+(?:[.,]\d+)?)\s*cm",
        'mm_generic': r"(\d+(?:[.,]\d+)?)\s*mm",
        'cm_generic': r"(\d+(?:[.,]\d+)?)\s*cm"
    }
   
    # 2. PRIMERA PRIORIDAD: Buscar medidas en formato NxN
    if product_type == "sello":
        medidas_match = re.search(patterns['medidas'], description)
        if medidas_match:
            medida1 = medidas_match.group(1) if medidas_match.group(1) else medidas_match.group(3)
            medida2 = medidas_match.group(2) if medidas_match.group(2) else medidas_match.group(4)
            if medida1 and medida2:
                medida = f"{format_measure(medida1)}x{format_measure(medida2)}"
                if "grabado" in description[:description.find("x")]:
                    metafields['medidas_zona_grabado'] = medida
                else:
                    metafields['medidas_chaton'] = medida
           
    elif product_type == "aros":
        # Buscar todas las medidas en formato NxN
        all_measures = list(re.finditer(patterns['medidas'], description))
       
        for match in all_measures:
            medida1 = match.group(1) if match.group(1) else match.group(3)
            medida2 = match.group(2) if match.group(2) else match.group(4)
           
            if medida1 and medida2:
                medida1 = float(normalize_number(medida1))
                medida2 = float(normalize_number(medida2))
                proporcion = medida1 / medida2 if medida2 != 0 else float('inf')

                if proporcion >= 3:
                    metafields['diametro'] = normalize_number(str(medida1))
                    metafields['grosor'] = normalize_number(str(medida2))
                else:
                    metafields['alto'] = normalize_number(str(medida1))
                    metafields['ancho'] = normalize_number(str(medida2))
                    metafields['medidas'] = f"{format_measure(str(medida1))}x{format_measure(str(medida2))}"

    else:  # Para otros tipos de producto
        medidas_match = re.search(patterns['medidas'], description)
        if medidas_match:
            medida1 = medidas_match.group(1) if medidas_match.group(1) else medidas_match.group(3)
            medida2 = medidas_match.group(2) if medidas_match.group(2) else medidas_match.group(4)
            if medida1 and medida2:
                alto = normalize_number(medida1)
                ancho = normalize_number(medida2)
                metafields['alto'] = alto
                metafields['ancho'] = ancho
                metafields['medidas'] = f"{format_measure(alto)}x{format_measure(ancho)}"

    # Comprobar si hay una medida de largo explícita
    largo_match = re.search(patterns['largo'], description)
    if largo_match and product_type in TIPOS_CON_LARGO:
        largo = float(normalize_number(largo_match.group(1)))
        if largo > 10:
            metafields['largo'] = str(largo)

    # 3. SEGUNDA PRIORIDAD: longitud total
    if "longitud total" in description and 'largo' not in metafields:
        match = re.search(patterns['longitud_total'], description)
        if match:
            largo = float(normalize_number(match.group(1)))
            if largo > 10:
                metafields['largo'] = str(largo)
   
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
        if cm_matches and product_type in TIPOS_CON_LARGO:
            largo = float(normalize_number(cm_matches[0]))
            if largo > 10:
                metafields['largo'] = str(largo)
   
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
            elif product_type in ["colgante", "medalla", "escapulario", "cristo", "horoscopo", "disco"]:
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

def normalize_text(text: str) -> str:
    """
    Normaliza texto: elimina acentos y convierte a minúsculas
    
    Args:
        text: Texto a normalizar
    
    Returns:
        str: Texto normalizado
    """
    import unicodedata
    # Normalizar los caracteres Unicode (NFD) y eliminar los diacríticos
    normalized = unicodedata.normalize('NFD', text.lower()).encode('ascii', 'ignore').decode('utf-8')
    return normalized.strip()

def extract_shapes_and_letters(description: str, product_type: str, title: str) -> dict:
    """
    Extrae formas de pendientes/colgantes y letras del título/descripción.
    Prioriza formas específicas (más largas) sobre formas genéricas y devuelve solo una forma.
    """
    metafields = {}
    
    # Solo procesar para pendientes y colgantes
    product_type = product_type.lower().strip()
    if product_type not in ['pendientes', 'colgante', 'collar', 'cadena', 'gargantilla', 'pulsera']:
        return metafields

    # Determinar la clave del metafield según el tipo de producto
    if product_type in ['gargantilla', 'collar', 'cadena']:
        metafield_key = 'forma_colgante'
    else:
        metafield_key = f"forma_{product_type}"

    # Inicializar con valor por defecto
    metafields[metafield_key] = 'Sin definir'

    # Normalizar textos para búsqueda    
    normalized_description = normalize_text(description)
    normalized_title = normalize_text(title)
    text_to_search = f"{normalized_description} {normalized_title}"
    
    # Diccionario de formas y sus variaciones (con sus versiones normalizadas)
    shapes = {
        'Aguila': ['aguila', 'aguilas'],
        'Amor': ['amor'],
        'Ancla': ['ancla', 'anclas'],
        'Angel': ['angel', 'angeles'],
        'Angelito Burlon': ['angelito burlon'],
        'Arbol De La Vida': ['arbol de la vida'],
        'Bellota': ['bellota', 'bellotas'],
        'Binzaga': ['binzaga', 'binzagas'],
        'Bola': ['bola', 'bolas'],
        'Bolso': ['bolso'],
        'Boton': ['boton', 'botones'],
        'Bruja': ['bruja', 'brujas'],
        'Buho': ['buho'],
        'Caballo': ['caballo', 'caballos'],
        'Camafeo': ['camafeo'],
        'Camaron De La Isla': ['camaron de la isla'],
        'Candado': ['candado', 'candados'],
        'Cereza': ['cereza', 'cerezas'],
        'Cigarra': ['cigarra', 'cigarras', 'cigarron', 'cigarrones'],
        'Clave De Sol': ['clave de sol'],
        'Conejo': ['conejo', 'conejito', 'conejos', 'conejitos'],
        'Corazon': ['corazon', 'corazones'],
        'Corazon Partido': ['corazon partido'],
        'Correcaminos': ['correcaminos'],
        'Coyote': ['coyote'],
        'Cuadrado': ['cuadrado', 'cuadrados'],
        'Cuajo': ['cuajo', 'cuajos'],
        'Cuerno': ['cuerno', 'cuernos'],
        'Delfin': ['delfin', 'delfines'],
        'Dolar': ['dolar', 'dolares'],
        'Dos Cuerpos': ['dos cuerpos'],
        'Elefante': ['elefante', 'elefantes'],
        'Esfinge': ['esfinge', 'esfinges'],
        'Estrella': ['estrella', 'estrellas'],
        'Estrella De David': ['estrella de david'],
        'Flor': ['flor', 'flores'],
        'Gallina': ['gallina', 'gallinas'],
        'Gato': ['gato', 'gatito', 'gatitos', 'gatita', 'gatitas', 'kitty'],
        'Girasol': ['girasol', 'girasoles'],
        'Gota': ['gota', 'gotas'],
        'Grupo Sanguineo': ['grupo sanguineo'],
        'Herradura': ['herradura', 'herraduras'],
        'Hexagono': ['hexagono', 'hexagonos'],
        'Higa': ['higa', 'higas'],
        'Hipopotamo': ['hipopotamo', 'hipopotamos'],
        'Hoja': ['hoja', 'hojas', 'hojitas', 'hojita'],
        'Indio': ['indio', 'indios'],
        'Infinito': ['infinito'],
        'Isla Gaudalupe': ['isla gaudalupe'],
        'Isla Martinica': ['isla martinica'],
        'Isla Reunion': ['isla reunion'],
        'Jai': ['jai'],
        'Lagrima': ['lagrima', 'lagrimas'],
        'Lauburu': ['lauburu'],
        'Leon': ['leon', 'leones'],
        'Letra': ['letra', 'letras', 'inicial', 'iniciales'],
        'Libelula': ['libelula', 'libelulas'],
        'Lingote': ['lingote', 'lingotes'],
        'Llave': ['llave', 'llaves'],
        'Media luna': ['media luna'],
        'Luna': ['luna', 'lunas'],
        'Madre': ['madre', 'mama', 'mami'],
        'Magdalena': ['magdalena'],
        'Mamamundi': ['mamamundi'],
        'Mano': ['mano', 'manos'],
        'Mano De Fatima': ['mano de fatima'],
        'Marihuana': ['marihuana'],
        'Manzana': ['manzana', 'manzanas'],
        'Mapache': ['mapache', 'mapaches'],
        'Margarita': ['margarita', 'margaritas'],
        'Mariposa': ['mariposa', 'mariposas'],
        'Mariquita': ['mariquita', 'mariquitas'],
        'Medusa': ['medusa', 'medusas'],
        'Menorah': ['menorah'],
        'Moneda': ['moneda', 'monedas'],
        'Morcilla': ['morcilla', 'morcillas'],
        'Nefertiti': ['nefertiti'],
        'Niña': ['nina', 'ninas'],
        'Niño': ['nino', 'ninos'],
        'Nudo': ['nudo', 'nudos'],
        'Ojo Turco': ['ojo turco'],
        'Oso': ['oso', 'osos'],
        'Ovalado': ['ovalados', 'oval', 'ovalado'],
        'Paloma': ['paloma', 'palomas'],
        'Pistola': ['pistola', 'pistolas'],
        'Pato Lucas': ['pato lucas'],
        'Payaso': ['payaso', 'payasos'],
        'Petalo': ['petalo', 'petalos'],
        'Pez': ['pez', 'peces'],
        'Pie': ['pie', 'pies'],
        'Piña': ['pina', 'pinas'],
        'Piolin': ['piolin', 'piolines'],
        'Pollito': ['pollito', 'pollitos'],
        'Puma': ['puma', 'pumas'],
        'Pluma': ['pluma', 'plumas'],
        'Puñal': ['punal', 'punales'],
        'Rectangular': ['rectangular', 'rectang', 'rectangulares'],
        'Redondo': ['redondo', 'redondos'],
        'Rombo': ['rombo', 'rombos'],
        'Rosa De Los Vientos': ['rosa de los vientos'],
        'Roseta': ['roseta', 'rosetas'],
        'Roseton': ['roseton', 'rosetones'],
        'Sacerdote Egipcio': ['sacerdote egipcio'],
        'San Rafael': ['san rafael'],
        'Serpiente': ['serpiente', 'serpientes'],
        'Silvestre': ['silvestre'],
        'Sol': ['sol', 'soles'],
        'Te Quiero Mama': ['te quiero mama', 'mama te quiero'],
        'Tigre': ['tigre', 'tigres'],
        'Tortuga': ['tortuga', 'tortugas'],
        'Trebol': ['trebol', 'treboles'],
        'Triangulo': ['triangulo', 'triangulos'],
        'Tutankamon': ['tutankamon'],
        'Virgen': ['virgen', 'virgenes'],
        'Virgen Del Pilar': ['virgen del pilar', 'v. del pilar'],
        'Virgen Del Rocio': ['virgen del rocio', 'v. del rocio'],
        'Virgen Niña': ['virgen nina'],
        'Virtudes': ['virtudes'],
        'Zapato': ['zapato', 'zapatos']
    }

    normalized_shapes = {
        key: [normalize_text(var) for var in variations]
        for key, variations in shapes.items()
    }
    
    found_shapes = []
    
    # Buscar formas usando texto normalizado
    for shape_name, normalized_variations in normalized_shapes.items():
        for variation in normalized_variations:
            if variation in text_to_search:
                found_shapes.append((shape_name, len(variation)))
    
    if found_shapes:
        # Ordenar por longitud de la variación encontrada (descendente)
        found_shapes.sort(key=lambda x: x[1], reverse=True)
        # Asignar la forma encontrada (sobreescribe 'Sin definir')
        metafields[metafield_key] = found_shapes[0][0]

    # Buscar letras en el título para colgantes
    if product_type == 'colgante' and ('letra' in normalized_title or 'inicial' in normalized_title):
        words = title.split()
        trigger_words = ['letra', 'inicial']
        trigger_pos = -1
        
        for trigger in trigger_words:
            if trigger in [normalize_text(w) for w in words]:
                trigger_pos = [normalize_text(w) for w in words].index(trigger)
                break
                
        if trigger_pos != -1:
            for i in range(trigger_pos, min(trigger_pos + 4, len(words))):
                clean_word = re.sub(r'[^A-Za-z]', '', words[i])
                if len(clean_word) == 1 and clean_word.isalpha():
                    metafields['letra'] = clean_word.upper()
                    break
    
    return metafields

def extract_medal_figure(description: str, product_type: str) -> dict:
    """
    Extrae la figura de la medalla o colgante del título/descripción.
    
    Args:
        description: Descripción o título del producto
        product_type: Tipo de producto ('medalla' o 'colgante')
        
    Returns:
        dict: Diccionario con el metafield correspondiente
    """
    if product_type.lower() not in ['medalla', 'colgante']:
        return {}

    # Diccionario de figuras y sus variantes
    figures = {
        'Ala es grande': ['ala es grande'],
        'Amor maternal': ['amor maternal'],
        'Angel Burlon': ['angel burlon', 'angelito burlon', 'angel burlón'],
        'Angel de la guarda': ['angel de la guarda', 'yo te guardare', 'te guardare'],
        'Angel niña revoltosa': ['angel niña revoltosa', 'angel nina revoltosa'],
        'Angel niño de la flor': ['angel niño de la flor', 'angel nino de la flor'],
        'Angel niño en la nube': ['angel niño en la nube', 'angel nino en la nube'],
        'Angel niño rezando': ['angel rezando','angel niño rezando', 'angel nino rezando', 'angel niño piadoso', 'angel nino piadoso'],
        'Angel Querubin': ['angel querubin'],
        'Angel yo te guardaré': ['angel yo te guardare'],
        'Angel': [r'\bangel\b'],
        'Arbol de la vida': ['arbol de la vida'],
        'Bautismo': ['bautismo'],
        'Corazón de Jesús': ['corazon de jesus', 'corazón de jesús'],
        'Espiritu santo': ['espiritu santo'],
        'Fray Leopoldo': ['fray leopoldo'],
        'Jesús del Gran Poder': ['jesus del gran poder', 'gran poder'],
        'Madonna del Mare Boticelli': ['madonna del mare', 'boticelli'],
        'Madre Divina Ternura': ['madre divina ternura', 'ternura'],
        'Medusa': ['medusa'],
        'Niño de Comunion': ['niño de comunion', 'nino de comunion'],
        'Niño de la hora': ['niño de la hora', 'nino de la hora'],
        'Niño del pesebre': ['niño del pesebre', 'nino del pesebre'],
        'Niño del remedio': ['niño del remedio', 'nino del remedio'],
        'Niño rezando': ['niño rezando', 'nino rezando'],
        'Notre Dame': ['notre dame'],
        'Nuestra Señora de Begoña': ['señora de begoña', 'senora de begona', 'begoña', 'begona'],
        'Nuestra Señora de la Luz': ['señora de la luz', 'senora de la luz'],
        'Nuestra Señora de Valvanera': ['señora de valvanera', 'senora de valvanera', 'valvanera'],
        'Regina Caelorum': ['regina caelorum'],
        'Reina de los cielos': ['reina de los cielos'],
        'Sagrado Corazon': ['sagrado corazon', 'sagrado corazón'],
        'Saint Michel': ['saint michel'],
        'San Antonio': ['san antonio'],
        'San Benito': ['san benito'],
        'San Cristobal': ['san cristobal'],
        'San Fermin': ['san fermin'],
        'San Francisco': ['san francisco'],
        'San Jorge': ['san jorge'],
        'San José': ['san jose', 'san josé'],
        'San Jose María Escrivá': ['san jose maria escriva', 'san josé maría escrivá'],
        'San Juan Pablo II': ['san juan pablo', 'juan pablo ii'],
        'San Judas Tadeo': ['san judas tadeo'],
        'San Lazaro': ['san lazaro'],
        'San Miguel': ['san miguel'],
        'San Vicente Ferrer': ['san vicente ferrer'],
        'Sant Benoit': ['sant benoit'],
        'Santa Faz': ['santa faz'],
        'Santa Gema': ['santa gema'],
        'Santa Lucia': ['santa lucia'],
        'Santa Teresa': ['santa teresa'],
        'Santiago Apostol': ['santiago apostol'],
        'Virgen de Africa': [r'virgen de africa\b', r'\bafrica\b'],
        'Virgen de Castellar': ['virgen de castellar', 'castellar'],
        'Virgen de Covadonga': ['virgen de covadonga', 'covadonga'],
        'Virgen de Fatima': ['virgen de fatima'],
        'Virgen de Guadalupe': ['virgen de guadalupe', 'virgen guadalupe'],
        'Virgen de la Almudena': ['virgen de la almudena', 'almudena'],
        'Virgen de la Altagracia': ['virgen de la altagracia', 'altagracia'],
        'Virgen de la asuncion': ['virgen de la asuncion'],
        'Virgen de la Cabeza': ['virgen de la cabeza'],
        'Virgen de la Candelaria': ['virgen de la candelaria', 'candelaria'],
        'Virgen de la Caridad': ['virgen de la caridad'],
        'Virgen de la cinta': ['virgen de la cinta', 'cinta'],
        'Virgen de la Macarena': ['virgen de la macarena', 'macarena'],
        'Virgen de la Merced': ['virgen de la merced', 'merced'],
        'Virgen de la Milagrosa': ['virgen de la milagrosa', 'virgen milagrosa'],
        'Virgen de la Oliva': ['virgen de la oliva', 'oliva'],
        'Virgen de la Paloma': ['virgen de la paloma', 'paloma'],
        'Virgen de las Angustias': ['virgen de las angustias', 'angustias'],
        'Virgen de las Nieves': ['virgen de las nieves', 'nieves'],
        'Virgen de Linarejos': ['virgen de linarejos', 'linarejos'],
        'Virgen de los Desamparados': ['virgen de los desamparados', 'desamparados'],
        'Virgen de Montserrat': ['virgen de montserrat', 'montserrat'],
        'Virgen de Tiscar': ['virgen de tiscar'],
        'Virgen del Carmen': ['virgen del carmen', 'virgen maria del carmen'],
        'Virgen del Mar': ['virgen del mar', r'\bmar\b'],
        'Virgen del Perpetuo Socorro': ['virgen del perpetuo socorro', 'socorro'],
        'Virgen del Pilar': ['virgen del pilar', r'\bpilar\b'],
        'Virgen del Pino': ['virgen del pino'],
        'Virgen del Prado': ['virgen del prado'],
        'Virgen del Quiche': ['virgen del quiche', 'virgen del quinche', 'quinche'],
        'Virgen del Rocio': ['virgen del rocio', r'\brocio\b'],
        'Virgen Inmaculada': ['virgen inmaculada'],
        'Virgen Macarena': ['virgen macarena', 'macarena'],
        'Virgen María Francesa': ['virgen maria francesa', 'virgen francesa', 'maria francesa'],
        'Virgen Milagrosa': ['virgen milagrosa'],
        'Virgen Negra': ['virgen negra'],
        'Virgen niña rezando': ['virgen niña rezando', 'virgen nina rezando'],
        'Virgen Niña': ['virgen niña', 'virgen nina'],
        'Virgen Pastora': ['virgen pastora'],
        'Virgen Rezando': ['virgen rezando', 'virgen maria rezando'],
        'Virgo Virginum': ['virgo virginum']
    }

    metafields = {}
    normalized_desc = normalize_text(description)

    # Buscar figuras en la descripción normalizada
    for figure_name, variations in figures.items():
        for variation in variations:
            # Usar expresiones regulares para coincidencias exactas de palabras
            if re.search(fr'\b{variation}\b', normalized_desc, re.IGNORECASE):
                metafield_key = 'figura_medalla' if product_type.lower() == 'medalla' else 'forma_colgante'
                metafields[metafield_key] = figure_name
                return metafields

    return metafields

def extract_medal_type(description: str, product_type: str) -> dict:
    """
    Extrae el tipo de medalla del título/descripción.
    
    Args:
        description: Descripción o título del producto
        product_type: Tipo de producto
        
    Returns:
        dict: Diccionario con el metafield de tipo de medalla
    """
    if product_type.lower() != 'medalla':
        return {}

    # Diccionario de tipos y sus variantes
    types = {
        'Calada': ['calada', 'calado', 'caladas'],
        'Con bisel': ['bisel', 'biselado', 'biselada'],
        'Cerco': ['cerco'],
        'Gota': ['gota'],
        'Lagrima': ['lagrima', 'lágrima'],
        'Tallada': ['tallada', 'tallado', 'talla'],
        'Oval': ['oval'],
        'Silueta': ['silueta'],
        'Filigrana': ['filigrana'],
        'Greca': ['greca'],
        'Escudo': ['escudo']
    }

    metafields = {}
    normalized_desc = normalize_text(description)

    # Buscar tipos en la descripción normalizada
    for type_name, variations in types.items():
        for variation in variations:
            if re.search(fr'\b{variation}\b', normalized_desc, re.IGNORECASE):
                metafields['tipo_medalla'] = type_name
                return metafields

    return metafields

def extract_pendant_type(description: str, product_type: str) -> dict:
    """
    Extrae el tipo de pendiente del título/descripción
    """
    if product_type.lower() != 'pendientes':  # Cambiado a plural
        return {}

    types = {
        'Cubana': ['cubana', 'cubanas'],
        'De perlas': ['perla', 'perlas'],
        'Largos': ['largos', 'largo'],
        'Tu y yo': ['tu y yo'],
        'Aro': ['aro', 'aros'],
        'Con bolas': ['bola', 'bolas'],
        'Orla': ['orla', 'orlas'],
        'Para novia': ['novia', 'novias'],
        'Para comunion': ['comunion', 'comuniones'],
        'Con banda': ['banda', 'bandas'],
        'Chaton': ['chaton', 'chatones'],
        'De garra': ['garra', 'garras'],
        'Morcilla': ['morcilla', 'morcillas'],
        'Calados': ['calado', 'calados'],
        'Tallados': ['tallado', 'tallados'],
        'Trepadores': ['trepador', 'trepadores']
    }

    description_lower = description.lower()
    
    for type_name, variations in types.items():
        for variant in variations:
            if variant in description_lower:
                return {'tipo_pendientes': type_name}

    return {}

def extract_chain_type(description: str, product_type: str) -> dict:
    """
    Extrae el tipo de cadena del título/descripción
    
    Args:
        description: Descripción o título del producto
        product_type: Tipo de producto
        
    Returns:
        dict: Diccionario con los metafields de tipo_eslabon y tipo_cadena
    """
    if product_type.lower() not in ['cadena', 'collar', 'cordon']:
        return {}

    # Diccionario de tipos y sus variantes
    types = {
        'Bilbao': ['bilbao'],
        'Singapur': ['singapur', 'singapure', 'singapur'],
        'Barbada': ['barbada','barbado'],
        'Cartier': ['cartier'],
        'Diamantada': ['diamantada','diamantado'],
        'Forzada': ['forzada', 'forzadas', 'forzado', 'forzados'],
        'Salomonico': ['salomonico', 'salomonica']
    }

    metafields = {}
    normalized_desc = normalize_text(description)

    # Buscar tipos de eslabón
    tipo_cadena = 'Otras'  # Valor por defecto para tipo_eslabon
    for type_name, variations in types.items():
        for variation in variations:
            if re.search(fr'\b{variation}\b', normalized_desc, re.IGNORECASE):
                tipo_cadena = type_name
                break
        if tipo_cadena != 'Otras':  # Si encontramos un tipo, salimos del bucle exterior
            break

    metafields['tipo_cadena'] = tipo_cadena
    # El tipo de cadena depende del tipo de eslabón
    metafields['cadena'] = 'Simple' if tipo_cadena != 'Otras' else 'Compuesta'

    return metafields