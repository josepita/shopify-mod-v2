#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de actualización de productos en Shopify desde CSV
Soporta actualización de todos los campos, incluyendo metafields e imágenes
"""

import requests
import pandas as pd
import json
import sys
import argparse
from typing import List, Dict, Tuple, Any, Optional
from dataclasses import dataclass
from collections import defaultdict
from colorama import init, Fore, Style
from config.settings import MYSQL_CONFIG, SHOPIFY_ACCESS_TOKEN, SHOPIFY_API_VERSION, SHOPIFY_SHOP_URL
from db.product_mapper import ProductMapper

# Inicializar colorama para formateo de colores
init()

@dataclass
class ProductChanges:
    """Clase para almacenar los cambios a realizar en un producto"""
    internal_reference: str
    title: str
    body_html: str = None
    new_tags: set = None
    new_images: List[Dict] = None
    metafields: List[Dict] = None

def clean_tag(tag: str) -> str:
    """Limpia un tag eliminando espacios innecesarios y caracteres no deseados"""
    return tag.strip().replace('  ', ' ')

def normalize_tags(tags_str: str) -> set:
    """Normaliza una cadena de tags a un conjunto de tags limpios"""
    if not tags_str or pd.isna(tags_str):
        return set()
    return {clean_tag(tag) for tag in tags_str.split(',') if tag.strip()}

def get_shopify_headers(access_token: str) -> Dict:
    """Devuelve los headers necesarios para las llamadas a Shopify"""
    return {
        'X-Shopify-Access-Token': access_token,
        'Content-Type': 'application/json'
    }

def get_shopify_base_url(shop_url: str, api_version: str) -> str:
    """Construye la URL base para las llamadas a Shopify"""
    return f'https://{shop_url}/admin/api/{api_version}'

def debug_print(title: str, data: Any, debug_mode: bool = False, indent: int = 0):
    """Imprime información de debug formateada"""
    if not debug_mode:
        return

    indent_str = "  " * indent
    print(f"\n{Fore.YELLOW}[DEBUG] {title}{Style.RESET_ALL}")
    
    if isinstance(data, dict):
        for key, value in data.items():
            print(f"{indent_str}  {Fore.CYAN}{key}{Style.RESET_ALL}: {value}")
    elif isinstance(data, (list, set)):
        for item in data:
            print(f"{indent_str}  - {item}")
    else:
        print(f"{indent_str}  {data}")

def print_comparison(label: str, current: str, new: str, indent: str = "  "):
    """Imprime una comparación entre valores actuales y nuevos"""
    if current != new:
        print(f"\n{Fore.CYAN}{label}:{Style.RESET_ALL}")
        print(f"{indent}Actual: {current}")
        print(f"{indent}Nuevo:  {Fore.GREEN}{new}{Style.RESET_ALL}")

def process_images_from_row(row: pd.Series) -> List[Dict]:
    """Procesa las imágenes de una fila del CSV"""
    images = []
    for i in range(1, 4):  # Procesar Image Src 1, 2 y 3
        img_src = row.get(f'Image Src {i}')
        if pd.notna(img_src):
            # Limpiar y validar URL
            img_src = str(img_src).strip()
            if not img_src.startswith(('http://', 'https://')):
                img_src = f"https://{img_src}"
                
            # Validar que la URL no tenga espacios ni caracteres inválidos
            img_src = img_src.replace(' ', '%20')
            
            images.append({
                'src': img_src,
                'position': i,
                'alt': f"{row.get('Title', '')} - Imagen {i}"
            })
    return images

def get_metafield_type(key: str, value: str) -> Tuple[str, Any]:
    """Determina el tipo correcto para un metafield basado en su clave y valor"""
    # Lista de campos que deben ser number_decimal
    decimal_fields = {'alto', 'ancho', 'grosor', 'largo', 'quilates'}
    
    # Si el campo debe ser decimal, convertir el valor
    if any(field in key.lower() for field in decimal_fields):
        try:
            # Limpiar el valor y convertir a decimal
            clean_value = str(value).replace(',', '.').strip()
            return 'number_decimal', float(clean_value)
        except (ValueError, TypeError):
            # Si no se puede convertir a decimal, usar 0
            return 'number_decimal', 0.0
    
    # Para el resto de campos, usar texto
    return 'single_line_text_field', str(value).strip()

def get_metafields_from_row(row: pd.Series) -> List[Dict]:
    """Extrae los metafields de una fila del CSV"""
    metafields = []
    metafield_columns = [col for col in row.index if col.startswith('product.metafields.custom.')]
    
    for column in metafield_columns:
        value = row[column]
        if pd.notna(value) and value != "NULL":
            key = column.replace('product.metafields.custom.', '')
            
            # Determinar tipo y valor correcto
            field_type, formatted_value = get_metafield_type(key, value)
            
            metafield = {
                'namespace': 'custom',
                'key': key,
                'value': str(formatted_value),  # Shopify espera siempre string
                'type': field_type
            }
            metafields.append(metafield)
    
    return metafields

def analyze_references(df: pd.DataFrame) -> Tuple[Dict[str, Dict], List[Dict]]:
    """Analiza las referencias en el DataFrame y devuelve información sobre duplicados"""
    reference_info = {}
    duplicated_rows = []
    
    for idx, row in df.iterrows():
        reference = row.get('Variant SKU', '')
        if pd.isna(reference) or reference == '':
            continue
            
        try:
            if str(reference).isdigit():
                reference = str(reference).zfill(4)  # Ajusta el padding según necesites
            else:
                reference = str(reference).strip()
        except:
            reference = str(reference).strip()
        
        if reference not in reference_info:
            reference_info[reference] = {
                'count': 1,
                'first_occurrence': row,
                'row_numbers': [idx + 1]
            }
        else:
            reference_info[reference]['count'] += 1
            reference_info[reference]['row_numbers'].append(idx + 1)
            duplicated_rows.append({
                'reference': reference,
                'row_number': idx + 1,
                'title': row.get('Title', 'Sin título')
            })
    
    return dict(sorted(reference_info.items())), duplicated_rows

def print_reference_analysis(reference_info: Dict[str, Dict], duplicated_rows: List[Dict]):
    """Imprime el análisis de referencias encontradas en el CSV"""
    total_unique_refs = len(reference_info)
    total_rows = sum(info['count'] for info in reference_info.values())
    duplicated_refs = len([ref for ref, info in reference_info.items() if info['count'] > 1])

    print(f"\n{Fore.CYAN}ANÁLISIS DE REFERENCIAS EN EL CSV:{Style.RESET_ALL}")
    print("="*80)
    print(f"Total de referencias únicas encontradas: {total_unique_refs}")
    print(f"Total de registros con referencia: {total_rows}")
    if duplicated_refs > 0:
        print(f"{Fore.YELLOW}Referencias con múltiples entradas: {duplicated_refs}{Style.RESET_ALL}")
    print("="*80)

    if duplicated_rows:
        print(f"\n{Fore.YELLOW}REFERENCIAS DUPLICADAS ENCONTRADAS:{Style.RESET_ALL}")
        print("="*80)
        print("Las siguientes entradas serán ignoradas:")
        for ref, info in reference_info.items():
            if info['count'] > 1:
                print(f"\nReferencia: {Fore.YELLOW}{ref}{Style.RESET_ALL}")
                print(f"Aparece en las filas: {', '.join(map(str, info['row_numbers']))}")
        print("="*80)

def get_product_by_reference(internal_reference: str, product_mapper: ProductMapper,
                           access_token: str, shop_url: str, api_version: str, 
                           debug_mode: bool = False) -> Optional[Dict]:
    """Obtiene un producto usando el sistema de mapeo"""
    try:
        if str(internal_reference).isdigit():
            internal_reference = str(internal_reference).zfill(4)
            
        print(f"\nBuscando producto con referencia: {internal_reference}")
        
        mapping = product_mapper.get_product_mapping(internal_reference)
        
        if not mapping:
            print(f"No se encontró mapeo para la referencia: {internal_reference}")
            return None

        product_id = mapping['product']['shopify_product_id']
        headers = get_shopify_headers(access_token)
        base_url = get_shopify_base_url(shop_url, api_version)
        
        # Obtener el producto
        url = f'{base_url}/products/{product_id}.json'
        response = requests.get(url, headers=headers)
        
        # Obtener metafields en una llamada separada
        metafields_url = f'{base_url}/products/{product_id}/metafields.json'
        metafields_response = requests.get(metafields_url, headers=headers)
        
        if response.status_code == 200 and metafields_response.status_code == 200:
            product = response.json()['product']
            metafields = metafields_response.json().get('metafields', [])
            
            # Añadir metafields al producto
            product['metafields'] = metafields
            
            if debug_mode:
                print(f"\nMetafields encontrados: {len(metafields)}")
                for m in metafields:
                    print(f"  • {m.get('namespace')}.{m.get('key')}: {m.get('value')} ({m.get('type')})")
            
            return product
        else:
            print(f"Error obteniendo producto de Shopify: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Error obteniendo producto: {str(e)}")
        return None

def update_product_with_metafields(product_id: int, update_data: Dict, metafields: List[Dict],
                                 access_token: str, shop_url: str, api_version: str,
                                 product_mapper: ProductMapper, demo_mode: bool = False,
                                 debug_mode: bool = False) -> bool:
    """Actualiza un producto y sus metafields en Shopify"""
    try:
        if demo_mode:
            return True
            
        # Filtrar imágenes inválidas
        valid_images = []
        for img in update_data.get('images', []):
            if img['src'] and all(c not in img['src'] for c in [' ', '<', '>', '"', "'"]):
                valid_images.append(img)
            else:
                print(f"Advertencia: Imagen ignorada por URL inválida: {img['src']}")
        
        headers = get_shopify_headers(access_token)
        base_url = get_shopify_base_url(shop_url, api_version)
        url = f'{base_url}/products/{product_id}.json'
        
        # Preparar datos para la actualización
        update_payload = {
            'product': {
                'id': product_id,
                'title': update_data['title'],
                'body_html': update_data['body_html'],
                'tags': update_data['tags'],
                'images': valid_images,
                'metafields': metafields or []
            }
        }
        
        if debug_mode:
            print(f"\nPayload de actualización: {json.dumps(update_payload, indent=2)}")
        
        response = requests.put(url, headers=headers, json=update_payload)
        
        if response.status_code != 200:
            print(f"Error actualizando producto: {response.status_code}")
            print(f"Respuesta: {response.text}")
            return False
            
        internal_reference = update_data.get('internal_reference')
        if internal_reference:
            product_mapper._log_sync(
                internal_reference=internal_reference,
                action='update_product',
                status='success',
                message=f'Product updated successfully. Title: {update_data.get("title")}'
            )
        
        return True
        
    except Exception as e:
        print(f"Error en update_product_with_metafields: {str(e)}")
        if 'internal_reference' in update_data:
            product_mapper._log_sync(
                internal_reference=update_data['internal_reference'],
                action='update_product',
                status='error',
                message=str(e)
            )
        return False
        
    except Exception as e:
        print(f"Error en update_product_with_metafields: {str(e)}")
        if 'internal_reference' in update_data:
            product_mapper._log_sync(
                internal_reference=update_data['internal_reference'],
                action='update_product',
                status='error',
                message=str(e)
            )
        return False

def print_debug_info(reference: str, current_product: Dict, update_data: Dict, 
                    changes: ProductChanges, product_mapper: ProductMapper, debug_mode: bool = False):
    """Muestra información detallada de debug del producto"""
    if not debug_mode:
        return

    print("\n" + "="*80)
    print(f"{Fore.YELLOW}[DEBUG] INFORMACIÓN DETALLADA{Style.RESET_ALL}")
    print("="*80)

    # Info de mapeo
    mapping = product_mapper.get_product_mapping(reference)
    if mapping:
        debug_print("MAPEO EN BASE DE DATOS", {
            'reference': reference,
            'shopify_id': mapping['product']['shopify_product_id'],
            'handle': mapping['product']['shopify_handle'],
            'created': mapping['product']['first_created_at'],
            'updated': mapping['product']['last_updated_at']
        }, debug_mode)

    # Info del producto
    debug_print("PRODUCTO SHOPIFY", {
        'id': current_product.get('id'),
        'handle': current_product.get('handle'),
        'status': current_product.get('status'),
        'created_at': current_product.get('created_at'),
        'updated_at': current_product.get('updated_at')
    }, debug_mode)

    # Cambios básicos
    debug_print("CAMBIOS EN CONTENIDO", {
        'Título actual': current_product.get('title'),
        'Título nuevo': update_data.get('title'),
        'HTML actual': current_product.get('body_html')[:100] + '...' if current_product.get('body_html') else None,
        'HTML nuevo': update_data.get('body_html')[:100] + '...' if update_data.get('body_html') else None,
    }, debug_mode)

    # Información detallada de imágenes
    print(f"\n{Fore.CYAN}IMÁGENES:{Style.RESET_ALL}")
    print("  Imágenes actuales:")
    current_images = current_product.get('images', [])
    if current_images:
        for idx, img in enumerate(current_images, 1):
            print(f"    {idx}. {img.get('src', 'N/A')}")
            print(f"       Position: {img.get('position', 'N/A')}")
            print(f"       ID: {img.get('id', 'N/A')}")
    else:
        print("    No tiene imágenes actualmente")

    print("\n  Imágenes nuevas:")
    new_images = changes.new_images or []
    if new_images:
        for idx, img in enumerate(new_images, 1):
            print(f"    {idx}. {img.get('src', 'N/A')}")
            print(f"       Position: {img.get('position', 'N/A')}")
            print(f"       Alt: {img.get('alt', 'N/A')}")
    else:
        print("    No se añadirán nuevas imágenes")

    # Información detallada de metafields
    print(f"\n{Fore.CYAN}METAFIELDS:{Style.RESET_ALL}")
    
    # Obtener metafields actuales
    current_metafields = {}
    for m in current_product.get('metafields', []):
        if m and isinstance(m, dict):
            key = f"{m.get('namespace')}.{m.get('key')}"
            current_metafields[key] = {
                'value': m.get('value'),
                'type': m.get('type')
            }

    # Obtener nuevos metafields
    new_metafields = {}
    for m in changes.metafields or []:
        if m and isinstance(m, dict):
            key = f"{m.get('namespace')}.{m.get('key')}"
            new_metafields[key] = {
                'value': m.get('value'),
                'type': m.get('type')
            }

    # Obtener todas las claves únicas
    all_keys = sorted(set(current_metafields.keys()) | set(new_metafields.keys()))

    if all_keys:
        print("\n  COMPARACIÓN DE METAFIELDS:")
        print("  " + "="*60)
        for key in all_keys:
            current_value = current_metafields.get(key, {}).get('value', 'NO EXISTE')
            new_value = new_metafields.get(key, {}).get('value', 'SIN CAMBIOS')
            current_type = current_metafields.get(key, {}).get('type', '')
            new_type = new_metafields.get(key, {}).get('type', current_type)

            # Determinar si hay cambio
            has_change = key in new_metafields and current_value != new_value
            
            # Mostrar todos los metafields, indicando si hay cambio
            print(f"\n  • {Fore.CYAN}{key}{Style.RESET_ALL}")
            if has_change:
                print(f"    Actual: {Fore.YELLOW}{current_value}{Style.RESET_ALL}")
                print(f"    Nuevo:  {Fore.GREEN}{new_value}{Style.RESET_ALL}")
            else:
                print(f"    Valor:  {current_value}")
            print(f"    Tipo:   {new_type or current_type}")
            
            if has_change:
                print(f"    {Fore.YELLOW}[SERÁ ACTUALIZADO]{Style.RESET_ALL}")
            
        print("  " + "="*60)
    else:
        print("  No hay metafields para comparar")

    # Tags
    current_tags = normalize_tags(current_product.get('tags', ''))
    new_tags = changes.new_tags or set()
    
    print(f"\n{Fore.CYAN}TAGS:{Style.RESET_ALL}")
    print("  Actuales:", ', '.join(sorted(current_tags)) if current_tags else 'Ninguno')
    
    tags_added = new_tags - current_tags
    tags_removed = current_tags - new_tags
    
    if tags_added:
        print(f"  {Fore.GREEN}Se añadirán: {', '.join(sorted(tags_added))}{Style.RESET_ALL}")
    if tags_removed:
        print(f"  {Fore.RED}Se eliminarán: {', '.join(sorted(tags_removed))}{Style.RESET_ALL}")
    if not (tags_added or tags_removed):
        print("  No hay cambios en los tags")

    # Variantes
    if current_product.get('variants'):
        print(f"\n{Fore.CYAN}VARIANTES:{Style.RESET_ALL}")
        for v in current_product.get('variants', []):
            print(f"  • SKU: {v.get('sku', 'N/A')}")
            print(f"    ID: {v.get('id', 'N/A')}")
            print(f"    Precio: {v.get('price', 'N/A')}")
            print(f"    Inventario: {v.get('inventory_quantity', 'N/A')}")

    print("\n" + "="*80)

def process_csv_updates(csv_path: str, access_token: str, shop_url: str, 
                       api_version: str, demo_mode: bool = False, debug_mode: bool = False):
    """Procesa el CSV y actualiza los productos"""
    product_mapper = None
    try:
        df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
        product_mapper = ProductMapper(MYSQL_CONFIG)
        
        reference_info, duplicated_rows = analyze_references(df)
        print_reference_analysis(reference_info, duplicated_rows)

        total_processed = 0
        total_success = 0
        total_errors = 0

        for reference, info in reference_info.items():
            row = info['first_occurrence']
            
            # Recopilar cambios
            changes = ProductChanges(
                internal_reference=reference,
                title=row['Title'],
                body_html=row['Body (HTML)'],
                new_tags=normalize_tags(row['Tags'])
            )

            # Procesar imágenes
            changes.new_images = process_images_from_row(row)

            # Procesar metafields
            changes.metafields = get_metafields_from_row(row)

            current_product = get_product_by_reference(
                reference, product_mapper, access_token, shop_url, api_version
            )
            
            if not current_product and not demo_mode:
                print(f"{Fore.RED}No se encontró el producto con referencia: {reference}{Style.RESET_ALL}")
                total_errors += 1
                continue

            total_processed += 1

            if demo_mode:
                if current_product:
                    print_debug_info(reference, current_product, 
                                   {'title': changes.title, 'body_html': changes.body_html}, 
                                   changes, product_mapper, debug_mode=True)
                continue

            if current_product:
                product_id = current_product['id']
                update_data = {
                    'title': changes.title,
                    'body_html': changes.body_html,
                    'tags': ','.join(sorted(changes.new_tags)) if changes.new_tags else '',
                    'internal_reference': reference,
                    'images': changes.new_images
                }
                
                if debug_mode:
                    print_debug_info(reference, current_product, update_data, 
                                   changes, product_mapper, debug_mode)
                
                if update_product_with_metafields(
                    product_id, update_data, changes.metafields,
                    access_token, shop_url, api_version,
                    product_mapper, demo_mode, debug_mode  # Añadido debug_mode aquí
                ):
                    print(f"{Fore.GREEN}Producto actualizado exitosamente: {reference}{Style.RESET_ALL}")
                    total_success += 1
                else:
                    print(f"{Fore.RED}Error actualizando producto: {reference}{Style.RESET_ALL}")
                    total_errors += 1

        # Mostrar resumen final
        print("\n" + "="*80)
        print(f"{Fore.CYAN}RESUMEN DE ACTUALIZACIÓN{Style.RESET_ALL}")
        print("="*80)
        print(f"Total productos procesados: {total_processed}")
        print(f"Actualizaciones exitosas: {Fore.GREEN}{total_success}{Style.RESET_ALL}")
        if total_errors > 0:
            print(f"Errores: {Fore.RED}{total_errors}{Style.RESET_ALL}")
        print("="*80)

    except Exception as e:
        print(f"{Fore.RED}Error procesando actualizaciones: {str(e)}{Style.RESET_ALL}")
    finally:
        if product_mapper:
            product_mapper.close()

def main():
    """Función principal del script"""
    parser = argparse.ArgumentParser(description='Actualiza productos en Shopify desde un CSV')
    parser.add_argument('csv_file', help='Ruta al archivo CSV con los datos de productos')
    parser.add_argument('--mode', choices=['update', 'screen'], default='screen',
                      help='Modo de ejecución: update para actualizar, screen para previsualizar')
    parser.add_argument('--debug', action='store_true',
                      help='Activa el modo debug con información detallada')
    args = parser.parse_args()

    try:
        process_csv_updates(
            csv_path=args.csv_file,
            access_token=SHOPIFY_ACCESS_TOKEN,
            shop_url=SHOPIFY_SHOP_URL,
            api_version=SHOPIFY_API_VERSION,
            demo_mode=(args.mode == 'screen'),
            debug_mode=args.debug
        )
    except Exception as e:
        print(f"{Fore.RED}Error en la ejecución: {str(e)}{Style.RESET_ALL}")
        sys.exit(1)

if __name__ == "__main__":
    main()    