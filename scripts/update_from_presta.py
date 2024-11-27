import requests
import pandas as pd
import json
import sys
import argparse
from typing import List, Dict, Tuple, Any
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
    new_images: List[str] = None
    metafields: List[Dict] = None

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
                reference = str(reference).zfill(4)
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
                           access_token: str, shop_url: str, api_version: str) -> Dict:
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
        url = f'{base_url}/products/{product_id}.json'
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            product = response.json()['product']
            print(f"Producto encontrado: {product['title']}")
            return product
        else:
            print(f"Error obteniendo producto de Shopify: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Error obteniendo producto: {str(e)}")
        return None
    
def print_comparison(label: str, current: str, new: str, indent: str = "  "):
    """Imprime una comparación entre valores actuales y nuevos"""
    if current != new:
        print(f"\n{Fore.CYAN}{label}:{Style.RESET_ALL}")
        print(f"{indent}Actual: {current}")
        print(f"{indent}Nuevo:  {Fore.GREEN}{new}{Style.RESET_ALL}")

def print_debug_info(reference: str, current_product: Dict, update_data: Dict, 
                    changes: ProductChanges, product_mapper: ProductMapper, debug_mode: bool = False):
    """Muestra información detallada de debug del producto"""
    if not debug_mode:
        return

    print("\n" + "="*80)
    print(f"{Fore.YELLOW}[DEBUG] INFORMACIÓN DETALLADA{Style.RESET_ALL}")
    print("="*80)

    mapping = product_mapper.get_product_mapping(reference)
    if mapping:
        debug_print("MAPEO EN BASE DE DATOS", {
            'reference': reference,
            'shopify_id': mapping['product']['shopify_product_id'],
            'handle': mapping['product']['shopify_handle'],
            'created': mapping['product']['first_created_at'],
            'updated': mapping['product']['last_updated_at']
        }, debug_mode)

    debug_print("PRODUCTO SHOPIFY", {
        'id': current_product.get('id'),
        'handle': current_product.get('handle'),
        'status': current_product.get('status'),
        'created_at': current_product.get('created_at'),
        'updated_at': current_product.get('updated_at')
    }, debug_mode)

    debug_print("CAMBIOS A REALIZAR", {
        'Título actual': current_product.get('title'),
        'Título nuevo': update_data.get('title'),
        'HTML actual': current_product.get('body_html')[:100] + '...' if current_product.get('body_html') else None,
        'HTML nuevo': update_data.get('body_html')[:100] + '...' if update_data.get('body_html') else None,
    }, debug_mode)

    if current_product.get('variants'):
        debug_print("VARIANTES", [
            {
                'id': v.get('id'),
                'sku': v.get('sku'),
                'price': v.get('price'),
                'inventory': v.get('inventory_quantity')
            }
            for v in current_product.get('variants', [])
        ], debug_mode)

    current_tags = set(current_product.get('tags', '').split(',')) if current_product.get('tags') else set()
    new_tags = changes.new_tags or set()
    if current_tags != new_tags:
        debug_print("TAGS", {
            'Actuales': list(current_tags),
            'Nuevos': list(new_tags),
            'Añadidos': list(new_tags - current_tags),
            'Eliminados': list(current_tags - new_tags)
        }, debug_mode)

    print("="*80)

def print_comparison_details(reference: str, current_product: Dict, changes: ProductChanges, reference_count: int):
    """Imprime una comparación detallada entre los valores actuales y los nuevos"""
    print("\n" + "="*80)
    print(f"{Fore.CYAN}COMPARATIVA DE CAMBIOS PARA REFERENCIA: {reference}{Style.RESET_ALL}")
    if reference_count > 1:
        print(f"{Fore.YELLOW}⚠️  ADVERTENCIA: Esta referencia aparece {reference_count} veces en el CSV. "
              f"Se usará solo la primera ocurrencia.{Style.RESET_ALL}")
    print("="*80)

    # Comparar título
    if current_product.get('title') != changes.title:
        print_comparison("TÍTULO", 
                        current_product.get('title', 'No disponible'),
                        changes.title)

    # Comparar descripción
    current_desc = current_product.get('body_html', 'No disponible')
    new_desc = changes.body_html or 'No disponible'
    if current_desc != new_desc:
        print_comparison("DESCRIPCIÓN", 
                        f"{current_desc[:150]}..." if len(current_desc) > 150 else current_desc,
                        f"{new_desc[:150]}..." if len(new_desc) > 150 else new_desc)

    # Comparar tags
    current_tags = set(current_product.get('tags', '').split(',')) if current_product.get('tags') else set()
    new_tags = changes.new_tags or set()
    
    if current_tags != new_tags:
        removed_tags = current_tags - new_tags
        added_tags = new_tags - current_tags
        
        print(f"\n{Fore.CYAN}ETIQUETAS:{Style.RESET_ALL}")
        print("  Actuales:", ', '.join(sorted(current_tags)) if current_tags else 'Ninguna')
        if removed_tags:
            print(f"  {Fore.RED}Se eliminarán: {', '.join(sorted(removed_tags))}{Style.RESET_ALL}")
        if added_tags:
            print(f"  {Fore.GREEN}Se añadirán: {', '.join(sorted(added_tags))}{Style.RESET_ALL}")

    # Comparar metafields
    if changes.metafields:
        current_metafields = {f"{m['namespace']}.{m['key']}": m['value'] 
                            for m in current_product.get('metafields', [])}
        
        new_metafields = {f"{m['namespace']}.{m['key']}": m['value'] 
                        for m in changes.metafields}
        
        changed_metafields = []
        for key, new_value in new_metafields.items():
            current_value = current_metafields.get(key, 'No existente')
            if current_value != new_value:
                changed_metafields.append((key, current_value, new_value))

        if changed_metafields:
            print(f"\n{Fore.CYAN}METAFIELDS QUE CAMBIARÁN:{Style.RESET_ALL}")
            for key, current_value, new_value in changed_metafields:
                print(f"  {key}:")
                print(f"    Actual: {current_value}")
                print(f"    Nuevo:  {Fore.GREEN}{new_value}{Style.RESET_ALL}")

    print("\n" + "="*80)

def get_metafields_from_row(row: pd.Series) -> List[Dict]:
    """Extrae los metafields de una fila del CSV"""
    metafields = []
    metafield_columns = [col for col in row.index if col.startswith('product.metafields.custom.')]
    
    for column in metafield_columns:
        value = row[column]
        if pd.notna(value) and value != "NULL":
            key = column.replace('product.metafields.custom.', '')
            metafield = {
                'namespace': 'custom',
                'key': key,
                'value': str(value),
                'type': 'single_line_text_field'
            }
            metafields.append(metafield)
    
    return metafields

def update_product_with_metafields(product_id: int, update_data: Dict, metafields: List[Dict],
                                 access_token: str, shop_url: str, api_version: str,
                                 product_mapper: ProductMapper, demo_mode: bool = False) -> bool:
    """Actualiza un producto y sus metafields en Shopify"""
    try:
        if demo_mode:
            return True
            
        headers = get_shopify_headers(access_token)
        base_url = get_shopify_base_url(shop_url, api_version)
        url = f'{base_url}/products/{product_id}.json'
        
        if metafields:
            update_data['metafields'] = metafields
            
        response = requests.put(
            url, 
            headers=headers, 
            json={'product': update_data}
        )
        
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

def process_csv_updates(csv_path: str, access_token: str, shop_url: str, 
                       api_version: str, demo_mode: bool = False, debug_mode: bool = False):
    """Procesa el CSV y actualiza los productos"""
    product_mapper = None
    try:
        df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
        product_mapper = ProductMapper(MYSQL_CONFIG)
        
        reference_info, duplicated_rows = analyze_references(df)
        print_reference_analysis(reference_info, duplicated_rows)

        for reference, info in reference_info.items():
            row = info['first_occurrence']
            
            changes = ProductChanges(
                internal_reference=reference,
                title=row['Title'],
                body_html=row['Body (HTML)'],
                new_tags=set([tag.strip() for tag in row['Tags'].split(',') if tag]),
            )

            current_product = get_product_by_reference(
                reference, product_mapper, access_token, shop_url, api_version
            )
            
            if not current_product and not demo_mode:
                print(f"{Fore.RED}No se encontró el producto con referencia: {reference}{Style.RESET_ALL}")
                continue

            if demo_mode:
                if current_product:
                    print_comparison_details(reference, current_product, changes, info['count'])
                continue

            if current_product:
                product_id = current_product['id']
                update_data = {
                    'id': product_id,
                    'title': changes.title,
                    'body_html': changes.body_html,
                    'tags': ','.join(changes.new_tags) if changes.new_tags else '',
                    'internal_reference': reference
                }
                
                if debug_mode:
                    print_debug_info(reference, current_product, update_data, 
                                   changes, product_mapper, debug_mode)
                
                if update_product_with_metafields(
                    product_id, update_data, changes.metafields,
                    access_token, shop_url, api_version,
                    product_mapper, demo_mode
                ):
                    print(f"{Fore.GREEN}Producto actualizado exitosamente: {reference}{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}Error actualizando producto: {reference}{Style.RESET_ALL}")

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