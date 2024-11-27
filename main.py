#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script principal de sincronización de productos con Shopify
Soporta productos simples y con variantes de talla
Maneja múltiples formatos de archivo (XLS, XLSX, CSV)
"""

import pandas as pd
import sys
import os
from typing import Dict, List, Optional, Tuple
import shopify
import time
from datetime import datetime
import logging
from pathlib import Path


# Importaciones locales
from config.settings import MYSQL_CONFIG, SHOPIFY_ACCESS_TOKEN, SHOPIFY_API_VERSION, SHOPIFY_SHOP_URL
from db.product_mapper import ProductMapper
from utils.helpers import (
    clean_value, format_price, validate_product_data, group_variants,
    format_title, process_tags, log_processing_stats, format_log_message,
    get_variant_size, extract_measures, extract_diamond_info, extract_stones  # Añadida aquí
)


def load_data(input_file: str) -> Optional[pd.DataFrame]:
    """
    Carga los datos desde un archivo Excel, HTML o CSV
    
    Args:
        input_file: Ruta del archivo a cargar
        
    Returns:
        Optional[pd.DataFrame]: DataFrame con los datos o None si hay error
    """
    print(f"\nIntentando cargar archivo: {input_file}")
    
    # Intentar como CSV primero
    try:
        # Probar diferentes encodings comunes
        encodings = ['utf-8', 'latin1', 'iso-8859-1']
        df = None
        
        for encoding in encodings:
            try:
                # Intentar con diferentes separadores comunes
                for separator in [',', ';', '\t']:
                    try:
                        df = pd.read_csv(input_file, encoding=encoding, sep=separator)
                        if len(df.columns) > 1:  # Verificar que se separó correctamente
                            logging.info(f"Archivo cargado como CSV (encoding: {encoding}, separador: {separator})")
                            df.columns = df.columns.str.strip()
                            logging.info(f"Columnas encontradas: {df.columns.tolist()}")
                            return df
                    except:
                        continue
            except:
                continue
                
        if df is None:
            logging.warning("No es un archivo CSV válido o el formato no es reconocido")
    except Exception as e:
        logging.error(f"Error al intentar leer como CSV: {str(e)}")

    # Intentar como Excel xlsx
    try:
        df = pd.read_excel(input_file, engine='openpyxl')
        logging.info("Archivo cargado como Excel XLSX")
        df.columns = df.columns.str.strip()
        logging.info(f"Columnas encontradas: {df.columns.tolist()}")
        return df
    except Exception as e:
        logging.warning(f"No es un archivo XLSX válido: {str(e)}")

    # Intentar como Excel xls
    try:
        df = pd.read_excel(input_file, engine='xlrd')
        logging.info("Archivo cargado como Excel XLS")
        df.columns = df.columns.str.strip()
        logging.info(f"Columnas encontradas: {df.columns.tolist()}")
        return df
    except Exception as e:
        logging.warning(f"No es un archivo XLS válido: {str(e)}")

    # Si llegamos aquí, no pudimos cargar el archivo
    logging.error(f"No se pudo cargar el archivo {input_file} en ningún formato soportado")
    return None

###########################################
# CONFIGURACIÓN DE SHOPIFY
###########################################

def setup_shopify_api() -> bool:
    """
    Configura la conexión con la API de Shopify
    
    Returns:
        bool: True si la conexión fue exitosa
    """
    try:
        logging.info("Iniciando configuración de API Shopify...")
        shop_url = SHOPIFY_SHOP_URL.replace('https://', '').replace('http://', '')
        api_url = f"https://{shop_url}/admin/api/{SHOPIFY_API_VERSION}"
        
        shopify.ShopifyResource.set_site(api_url)
        shopify.ShopifyResource.set_headers({
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
        })
        
        shop = shopify.Shop.current()
        logging.info(f"Conexión exitosa con la tienda: {shop.name}")
        return True
        
    except Exception as e:
        logging.error(f"Error de configuración Shopify: {e}")
        return False

def get_location_id() -> str:
    """
    Obtiene el ID de la ubicación principal de Shopify
    
    Returns:
        str: ID de la ubicación
    """
    locations = shopify.Location.find()
    if not locations:
        raise Exception("No se encontró ubicación para el inventario")
    return locations[0].id

###########################################
# FUNCIONES DE CREACIÓN DE PRODUCTOS
###########################################

def create_simple_product(
    product_data: Dict, 
    product_mapper: ProductMapper,
    location_id: str,
    is_update: bool = False
) -> bool:
    """
    Crea un producto simple (sin variantes) en Shopify
    """
    try:
        new_product = shopify.Product()
        new_product.title = product_data['title']
        new_product.body_html = product_data['body_html']
        new_product.vendor = product_data['vendor']
        new_product.product_type = product_data['product_type']
        new_product.tags = product_data['tags']
        new_product.published = True
        
        variant = shopify.Variant({
            'price': product_data['price'],
            'sku': product_data['sku'],
            'inventory_management': 'shopify',
            'inventory_policy': 'deny',
            'grams': int(float(product_data.get('weight', 0))),
            'cost': product_data.get('cost', 0)
        })
        
        new_product.variants = [variant]
        
        if new_product.save():
            # Guardar mapeo del producto
            success = product_mapper.save_product_mapping(
                internal_reference=product_data['sku'],
                shopify_product=new_product,
                is_update=is_update
            )
            
            if not success:
                raise Exception("Error guardando mapeo del producto")
            
            # Configurar inventario
            shopify.InventoryLevel.set(
                location_id=location_id,
                inventory_item_id=new_product.variants[0].inventory_item_id,
                available=product_data['stock']
            )
            
            # Crear metafields
            if product_data.get('metafields'):
                create_product_metafields(new_product.id, product_data['metafields'])
            
            # Configurar imágenes
            if product_data.get('images'):
                setup_product_images(new_product.id, product_data['images'])
            
            return True
        else:
            logging.error(f"Error al crear producto simple: {new_product.errors.full_messages()}")
            return False
            
    except Exception as e:
        logging.error(f"Error creando producto simple: {str(e)}")
        return False

def update_simple_product(
    product_data: Dict,
    shopify_id: int,
    product_mapper: ProductMapper,
    location_id: str,
    is_update: bool = True
) -> bool:
    """
    Actualiza un producto simple existente en Shopify
    """
    try:
        existing_product = shopify.Product.find(shopify_id)
        if not existing_product:
            logging.error(f"No se encontró el producto con ID {shopify_id}")
            return False

        existing_product.title = product_data['title']
        existing_product.body_html = product_data['body_html']
        existing_product.vendor = product_data['vendor']
        existing_product.product_type = product_data['product_type']
        existing_product.tags = product_data['tags']
        
        if existing_product.variants:
            variant = existing_product.variants[0]
            variant.price = product_data['price']
            variant.sku = product_data['sku']
            variant.inventory_management = 'shopify'
            variant.inventory_policy = 'deny'
            variant.grams = int(float(product_data.get('weight', 0)))
            variant.cost = product_data.get('cost', 0)
        
        if existing_product.save():
            # Guardar mapeo del producto
            success = product_mapper.save_product_mapping(
                internal_reference=product_data['sku'],
                shopify_product=existing_product,
                is_update=is_update
            )
            
            if not success:
                raise Exception("Error guardando mapeo del producto")
            
            # Configurar inventario
            shopify.InventoryLevel.set(
                location_id=location_id,
                inventory_item_id=existing_product.variants[0].inventory_item_id,
                available=product_data['stock']
            )
            
            # Actualizar metafields
            if product_data.get('metafields'):
                create_product_metafields(existing_product.id, product_data['metafields'])
            
            # Actualizar imágenes
            if product_data.get('images'):
                for image in existing_product.images:
                    image.destroy()
                setup_product_images(existing_product.id, product_data['images'])
            
            return True
        else:
            logging.error(f"Error al actualizar producto simple: {existing_product.errors.full_messages()}")
            return False
            
    except Exception as e:
        logging.error(f"Error actualizando producto simple: {str(e)}")
        return False

def create_variant_product(
    product_data: Dict, 
    variants_data: List[Dict], 
    product_mapper: ProductMapper,
    location_id: str
) -> bool:
    """
    Crea un producto con variantes en Shopify
    """
    try:
        print("Creando nuevo producto con variantes...")
        new_product = shopify.Product()
        new_product.title = product_data['title']
        new_product.body_html = product_data['body_html']
        new_product.vendor = product_data['vendor']
        new_product.product_type = product_data['product_type']
        new_product.tags = product_data['tags']
        new_product.published = True
        
        # Configurar opción de talla
        tallas = [v['size'] for v in variants_data]
        new_product.options = [{'name': 'Talla', 'values': sorted(list(set(tallas)))}]
        
        # Crear variantes
        variants = []
        for var_data in variants_data:
            # Convertir peso de gramos a valor entero y asegurarse que es un número válido
            try:
                weight_in_grams = float(var_data.get('weight', 0))
                weight_int = int(weight_in_grams * 1000)  # Convertir a miligramos
            except (ValueError, TypeError):
                weight_int = 0
                print(f"⚠️ Error convirtiendo peso para variante {var_data['sku']}")

            variant = shopify.Variant({
                'option1': var_data['size'],
                'price': var_data['price'],
                'sku': var_data['sku'],
                'inventory_management': 'shopify',
                'inventory_policy': 'deny',
                'grams': weight_int,  # Usar el peso convertido
                'weight': weight_in_grams,  # Peso original en gramos
                'weight_unit': 'g',  # Especificar unidad de peso
                'cost': var_data.get('cost', 0)
            })
            variants.append(variant)
            print(f"Variante creada - SKU: {var_data['sku']}, Peso: {weight_int}g")
            
        new_product.variants = variants
        
        print("Guardando producto base...")
        if not new_product.save():
            print(f"Error al crear producto base: {new_product.errors.full_messages()}")
            return False

        shopify_product_id = int(new_product.id)
        print(f"Producto base guardado con ID: {shopify_product_id}")
        
        # Guardar mapeo del producto
        print("Guardando mapeo del producto base...")
        if not product_mapper.save_product_mapping(
            internal_reference=product_data['sku'],
            shopify_product=new_product
        ):
            raise Exception("Error guardando mapeo del producto")

        # Guardar variantes y configurar inventario
        print(f"Procesando {len(new_product.variants)} variantes...")
        new_product.reload()  # Recargar para asegurarnos de tener toda la info actualizada
        
        for variant, var_data in zip(new_product.variants, variants_data):
            print(f"\nProcesando variante: {var_data['sku']}")
            print(f"- ID Variante: {variant.id}")
            print(f"- Talla: {var_data['size']}")
            print(f"- Stock: {var_data['stock']}")
            
            # Guardar mapeo de variante
            if not product_mapper.save_variant_mapping(
                internal_sku=var_data['sku'],
                variant=variant,
                parent_reference=product_data['sku'],
                shopify_product_id=shopify_product_id,
                size=var_data['size'],
                price=var_data['price']
            ):
                raise Exception(f"Error guardando mapeo de variante {var_data['sku']}")
            
            # Configurar inventario
            print(f"Configurando stock: {var_data['stock']} unidades")
            shopify.InventoryLevel.set(
                location_id=location_id,
                inventory_item_id=variant.inventory_item_id,
                available=var_data['stock']
            )

        # Crear metafields después de que todo lo demás esté listo
        if product_data.get('metafields'):
            print("\nCreando metafields...")
            create_product_metafields(shopify_product_id, product_data['metafields'])
        
        # Configurar imágenes al final
        if product_data.get('images'):
            print(f"\nConfigurando {len(product_data['images'])} imágenes...")
            setup_product_images(shopify_product_id, product_data['images'])
        
        print(f"\nProducto {product_data['sku']} creado completamente con éxito")
        return True
            
    except Exception as e:
        print(f"Error creando producto con variantes: {str(e)}")
        logging.error(f"Error creando producto con variantes: {str(e)}")
        return False

def update_variant_product(
    product_data: Dict, 
    variants_data: List[Dict],
    shopify_id: int,
    product_mapper: ProductMapper,
    location_id: str
) -> bool:
    """
    Actualiza un producto con variantes existente en Shopify
    """
    try:
        print(f"Obteniendo producto de Shopify con ID: {shopify_id}")
        existing_product = shopify.Product.find(shopify_id)
        if not existing_product:
            print(f"No se encontró el producto con ID {shopify_id}")
            return False

        print("Actualizando datos básicos del producto...")
        # Actualizar datos básicos del producto
        existing_product.title = product_data['title']
        existing_product.body_html = product_data['body_html']
        existing_product.vendor = product_data['vendor']
        existing_product.product_type = product_data['product_type']
        existing_product.tags = product_data['tags']
        
        # Configurar opción de talla
        print("Actualizando opciones de talla...")
        tallas = [v['size'] for v in variants_data]
        existing_product.options = [{'name': 'Talla', 'values': sorted(list(set(tallas)))}]
        
        print("Guardando cambios en producto base...")
        if not existing_product.save():
            print(f"Error al actualizar producto base: {existing_product.errors.full_messages()}")
            return False

        # Guardar mapeo del producto
        print("Actualizando mapeo del producto...")
        success = product_mapper.save_product_mapping(
            internal_reference=product_data['sku'],
            shopify_product=existing_product,
            is_update=True
        )
            
        if not success:
            raise Exception("Error guardando mapeo del producto")

        # Recargar producto para tener la información más actualizada
        existing_product.reload()
        
        # Crear diccionario de variantes existentes por SKU
        print("Procesando variantes...")
        existing_variants = {v.sku: v for v in existing_product.variants}
        new_variants = []
        
        # Procesar cada variante
        for var_data in variants_data:
            variant = None
            is_new_variant = var_data['sku'] not in existing_variants
            
            if is_new_variant:
                print(f"Creando nueva variante: {var_data['sku']}")
                variant = shopify.Variant({
                    'product_id': shopify_id,
                    'option1': var_data['size'],
                    'price': var_data['price'],
                    'sku': var_data['sku'],
                    'inventory_management': 'shopify',
                    'inventory_policy': 'deny',
                    'grams': int(float(var_data.get('weight', 0))),
                    'cost': var_data.get('cost', 0)
                })
            else:
                print(f"Actualizando variante existente: {var_data['sku']}")
                variant = existing_variants[var_data['sku']]
                variant.option1 = var_data['size']
                variant.price = var_data['price']
                variant.grams = int(float(var_data.get('weight', 0)))
                variant.cost = var_data.get('cost', 0)
            
            # Guardar/actualizar variante
            if not variant.save():
                print(f"Error guardando variante: {variant.errors.full_messages()}")
                continue
                
            # Guardar mapeo de variante
            success = product_mapper.save_variant_mapping(
                internal_sku=var_data['sku'],
                variant=variant,
                parent_reference=product_data['sku'],
                shopify_product_id=shopify_id,
                size=var_data['size'],
                price=var_data['price'],
                is_update=not is_new_variant
            )
            
            if not success:
                print(f"Error guardando mapeo de variante {var_data['sku']}")
                continue
            
            # Actualizar inventario
            print(f"Actualizando stock de {var_data['sku']} a {var_data['stock']} unidades")
            shopify.InventoryLevel.set(
                location_id=location_id,
                inventory_item_id=variant.inventory_item_id,
                available=var_data['stock']
            )
            
            new_variants.append(variant)

        # Actualizar metafields
        if product_data.get('metafields'):
            print("Actualizando metafields...")
            create_product_metafields(shopify_id, product_data['metafields'])
        
        # Actualizar imágenes
        if product_data.get('images'):
            print(f"Actualizando {len(product_data['images'])} imágenes...")
            # Eliminar imágenes existentes
            for image in existing_product.images:
                image.destroy()
            # Añadir nuevas imágenes
            setup_product_images(shopify_id, product_data['images'])
        
        print(f"Producto {product_data['sku']} actualizado exitosamente")
        return True
            
    except Exception as e:
        print(f"Error actualizando producto con variantes: {str(e)}")
        logging.error(f"Error actualizando producto con variantes: {str(e)}")
        return False

###########################################
# FUNCIONES DE METAFIELDS E IMÁGENES
###########################################

def create_product_metafields(product_id: int, metafields_data: Dict[str, str]) -> None:
    """
    Crea los metafields para un producto
    
    Args:
        product_id: ID del producto en Shopify
        metafields_data: Diccionario con los metafields a crear
    """
    # Mapeo de nombres internos a nombres de Shopify y sus tipos
    field_mapping = {
        'alto': {'key': 'alto', 'type': 'number_decimal'},
        'ancho': {'key': 'ancho', 'type': 'number_decimal'},
        'grosor': {'key': 'grosor', 'type': 'number_decimal'},
        'medidas': {'key': 'medidas', 'type': 'single_line_text_field'},
        'largo': {'key': 'largo', 'type': 'number_decimal'},
        'diametro': {'key': 'diametro', 'type': 'number_decimal'},
        'piedra': {'key': 'piedra', 'type': 'single_line_text_field'},
        'tipo_piedra': {'key': 'tipo_piedra', 'type': 'single_line_text_field'},
        'forma_piedra': {'key': 'forma_piedra', 'type': 'single_line_text_field'},
        'calidad_piedra': {'key': 'calidad_piedra', 'type': 'single_line_text_field'},
        'color_piedra': {'key': 'color_piedra', 'type': 'single_line_text_field'},
        'disposicion_piedras': {'key': 'disposicion_de_la_piedra', 'type': 'single_line_text_field'},
        'acabado': {'key': 'acabado', 'type': 'single_line_text_field'},
        'estructura': {'key': 'estructura', 'type': 'single_line_text_field'},
        'material': {'key': 'material', 'type': 'single_line_text_field'},
        'destinatario': {'key': 'destinatario', 'type': 'single_line_text_field'},
        'cierre': {'key': 'cierre', 'type': 'single_line_text_field'},
        'color_oro': {'key': 'color_oro', 'type': 'single_line_text_field'},
        'calidad_diamante': {'key': 'calidad_diamante', 'type': 'single_line_text_field'},
        'kilates_diamante': {'key': 'kilates_diamante', 'type': 'number_decimal'},
        'color_diamante': {'key': 'color_diamante', 'type': 'single_line_text_field'}
    }

    for internal_key, value in metafields_data.items():
        if value and str(value).strip():
            try:
                field_config = field_mapping.get(internal_key)
                if not field_config:
                    continue
                
                shopify_key = field_config['key']
                field_type = field_config['type']
                
                # Formatear el valor según el tipo
                formatted_value = value
                if field_type == 'number_decimal':
                    formatted_value = str(float(str(value).replace(',', '.')))
                
                metafield = shopify.Metafield({
                    'namespace': 'custom',
                    'key': shopify_key,
                    'value': formatted_value,
                    'type': field_type,
                    'owner_id': product_id,
                    'owner_resource': 'product'
                })
                
                if metafield.save():
                    logging.info(f"Metafield creado: {shopify_key} = {formatted_value}")
                else:
                    logging.error(f"Error al crear metafield {shopify_key}: {metafield.errors.full_messages()}")
                    
            except Exception as e:
                logging.error(f"Error creando metafield {internal_key}: {str(e)}")

def setup_product_images(product_id: int, image_data: List[Dict]) -> None:
    """
    Configura las imágenes del producto
    
    Args:
        product_id: ID del producto en Shopify
        image_data: Lista de diccionarios con datos de imágenes
    """
    for img_data in image_data:
        if img_data.get('src'):
            try:
                image = shopify.Image({
                    'product_id': product_id,
                    'src': img_data['src'],
                    'position': img_data['position'],
                    'alt': img_data.get('alt', '')
                })
                image.save()
            except Exception as e:
                logging.error(f"Error configurando imagen: {str(e)}")

###########################################
# FUNCIÓN PRINCIPAL DE PROCESAMIENTO
###########################################

def process_products(df: pd.DataFrame, display_mode: bool = False) -> None:
    """
    Procesa los productos del DataFrame
    
    Args:
        df: DataFrame con los productos a procesar
        display_mode: Si es True, solo muestra información sin crear productos
    """
    products_processed = 0
    products_failed = 0
    product_mapper = ProductMapper(MYSQL_CONFIG)
    start_time = datetime.now()

    try:
        location_id = None
        if not display_mode:
            location_id = get_location_id()
        
        grouped_products = group_variants(df)
        total_products = len(grouped_products)
        
        logging.info(f"Total de productos a procesar: {total_products}")
        
        for i, (base_reference, product_info) in enumerate(grouped_products.items(), 1):
            product_start_time = datetime.now()
            
            try:
                base_row = product_info['base_data']
                product_data = prepare_product_data(base_row, base_reference)
                
                print(f"\n{'='*50}")
                print(f"PRODUCTO {i} DE {total_products}")
                print(f"{'='*50}")
                
                # Mostrar los datos del producto antes de procesar
                print(f"Procesando producto {base_reference}:")
                print(f"  - Título: {product_data['title']}")
                print(f"  - SKU: {product_data['sku']}")
                print(f"  - Tipo: {product_data['product_type']}")
                print(f"  - Precio: {product_data['price']} EUR")
                print(f"  - Stock: {product_data['stock']}")
                print(f"  - Metafields: {product_data['metafields']}")
                print(f"  - Imágenes: {[img['src'] for img in product_data['images']]}")

                if not display_mode:
                    # Verificar si el producto existe
                    existing_mapping = product_mapper.get_product_mapping(base_reference)
                    print("\n" + "="*50)

                    if existing_mapping:
                        print(f"🔄 PRODUCTO EXISTENTE EN SHOPIFY")
                        print(f"ID Shopify: {existing_mapping['product']['shopify_product_id']}")
                        print(f"Handle: {existing_mapping['product']['shopify_handle']}")
                        print(f"Título actual: {existing_mapping['product']['title']}")
                    else:
                        print(f"🆕 PRODUCTO NUEVO - NO EXISTE EN SHOPIFY")
                    print("="*50 + "\n")

                    if product_info['is_variant_product']:
                        variants_data = prepare_variants_data(product_info['variants'])
                        if existing_mapping:
                            shopify_id = existing_mapping['product']['shopify_product_id']
                            success = update_variant_product(
                                product_data, 
                                variants_data,
                                shopify_id,
                                product_mapper, 
                                location_id
                            )
                        else:
                            success = create_variant_product(
                                product_data, 
                                variants_data, 
                                product_mapper, 
                                location_id
                            )
                    else:
                        if existing_mapping:
                            shopify_id = existing_mapping['product']['shopify_product_id']
                            success = update_simple_product(
                                product_data, 
                                shopify_id,
                                product_mapper, 
                                location_id
                            )
                        else:
                            success = create_simple_product(
                                product_data, 
                                product_mapper, 
                                location_id
                            )

                    if success:
                        print(f"✅ Producto {base_reference} {'actualizado' if existing_mapping else 'creado'} con éxito.")
                        products_processed += 1
                    else:
                        print(f"❌ Error al {'actualizar' if existing_mapping else 'crear'} producto {base_reference}.")
                        products_failed += 1

                    # Cálculos de tiempo
                    product_end_time = datetime.now()
                    product_duration = (product_end_time - product_start_time).total_seconds()
                    total_duration = (product_end_time - start_time).total_seconds()
                    products_remaining = total_products - i
                    avg_time_per_product = total_duration / i
                    estimated_time_remaining = products_remaining * avg_time_per_product

                    print("\n" + "="*50)
                    print("ESTADÍSTICAS DE TIEMPO")
                    print(f"⏱️  Tiempo producto actual: {product_duration:.1f} segundos")
                    print(f"⏳ Tiempo promedio/producto: {avg_time_per_product:.1f} segundos")
                    print(f"🎯 Tiempo restante estimado: {estimated_time_remaining/60:.1f} minutos")
                    print(f"📊 Progreso: {i}/{total_products} ({(i/total_products*100):.1f}%)")
                    print("="*50)

                    # Esperar entre solicitudes para evitar límites de API
                    time.sleep(1)
                    
            except Exception as e:
                logging.error(f"Error procesando producto {base_reference}: {str(e)}")
                print(f"❌ Error procesando producto {base_reference}: {str(e)}\n")
                products_failed += 1

        # Resumen final
        total_time = (datetime.now() - start_time).total_seconds()
        print("\n" + "="*50)
        print("RESUMEN FINAL DE LA OPERACIÓN")
        print("="*50)
        print(f"✅ Productos procesados con éxito: {products_processed}")
        print(f"❌ Productos fallidos: {products_failed}")
        print(f"⏱️  Tiempo total de ejecución: {total_time/60:.1f} minutos")
        if products_processed > 0:
            print(f"⌛ Tiempo promedio por producto: {total_time/products_processed:.1f} segundos")
        print("="*50)

    finally:
        product_mapper.close()


###########################################
# FUNCIONES DE PREPARACIÓN DE DATOS
###########################################

def prepare_product_data(base_row: pd.Series, base_reference: str) -> Dict:
    """
    Prepara los datos comunes del producto
    """
    # Extraer descripción y tipo
    description = clean_value(base_row['DESCRIPCION'])
    product_type = clean_value(base_row.get('TIPO', '')).lower()
    
    # Inicializar diccionario de metafields vacío
    metafields = {}

    # Extraer medidas
    measures = extract_measures(description, product_type)
    if measures:
        metafields.update(measures)

    # Extraer información de diamantes
    diamond_info = extract_diamond_info(description)
    if diamond_info:
        metafields.update(diamond_info)

    # Extraer información de piedras
    stones_info = extract_stones(description)
    if stones_info:
        metafields.update(stones_info)

    # Añadir campos básicos solo si tienen valor
    destinatario = clean_value(base_row.get('GENERO', '')).capitalize()
    if destinatario:
        metafields['destinatario'] = destinatario

    cierre = clean_value(base_row.get('CIERRE', '')).capitalize()
    if cierre:
        metafields['cierre'] = cierre

    material = get_material(base_row['DESCRIPCION'])
    if material:
        metafields['material'] = material

    color_oro = clean_value(base_row.get('COLOR ORO', '')).capitalize()
    if color_oro:
        metafields['color_oro'] = color_oro

    return {
        'title': format_title(base_reference, base_row['DESCRIPCION']),
        'body_html': description,
        'vendor': "Joyas Armaan",
        'product_type': clean_value(base_row['TIPO']).capitalize(),
        'tags': process_tags(
            base_row.get('CATEGORIA', ''),
            base_row.get('SUBCATEGORIA', ''),
            base_row.get('TIPO', '')
        ),
        'sku': base_reference,
        'price': round(float(base_row['PRECIO']) * 2.2, 2),
        'stock': int(base_row['STOCK']),
        'weight': clean_value(base_row.get('PESO G.', 0)),
        'cost': clean_value(base_row['PRECIO']),
        'metafields': metafields,
        'images': prepare_images_data(base_row)
    }

def prepare_variants_data(variants_rows: List[pd.Series]) -> List[Dict]:
    """
    Prepara los datos de las variantes
    
    Args:
        variants_rows: Lista de filas del DataFrame con datos de variantes
        
    Returns:
        List[Dict]: Lista de datos de variantes preparados
    """
    variants_data = []
    for row in variants_rows:
        variant_reference = clean_value(row['REFERENCIA'])
        size = get_variant_size(variant_reference)
        if size:
            # Limpiar y convertir el peso
            try:
                weight = float(clean_value(row.get('PESO G.', 0)).replace(',', '.'))
            except (ValueError, TypeError):
                weight = 0
                print(f"⚠️ Error en peso para variante {variant_reference}")

            variants_data.append({
                'size': size,
                'price': round(float(row['PRECIO']) * 2.2, 2),
                'sku': variant_reference,
                'stock': int(row['STOCK']),
                'weight': weight,  # Peso en gramos
                'cost': clean_value(row['PRECIO'])
            })
            
            print(f"Datos de variante preparados - SKU: {variant_reference}, Peso: {weight}g")
            
    return variants_data

def prepare_images_data(row: pd.Series) -> List[Dict]:
    """
    Prepara los datos de las imágenes
    
    Args:
        row: Fila del DataFrame con datos de imágenes
        
    Returns:
        List[Dict]: Lista de datos de imágenes preparados
    """
    images = []
    for idx, img_col in enumerate(['IMAGEN 1', 'IMAGEN 2', 'IMAGEN 3'], 1):
        img_src = clean_value(row.get(img_col, ''))
        if img_src:
            if not img_src.startswith(('http://', 'https://')):
                img_src = f"https://{img_src}"
            images.append({
                'src': img_src,
                'position': idx,
                'alt': f"{row.get('DESCRIPCION', '')} - Imagen {idx}"
            })
    return images

def get_material(description: str) -> str:
    """
    Determina el material basado en la descripción
    
    Args:
        description: Descripción del producto
        
    Returns:
        str: Material determinado
    """
    if isinstance(description, str):
        description = description.upper()
        if description.startswith("18K"):
            return "Oro 18 kilates"
        elif description.startswith("9K"):
            return "Oro 9 kilates"
    return ""

###########################################
# FUNCIÓN MAIN
###########################################

def main():
    """Función principal del script"""
    if len(sys.argv) != 3:
        print("""
Uso: python main.py <input_file> <mode>

Argumentos:
  input_file    - Archivo de entrada (Excel XLS/XLSX o CSV)
  mode          - Modo de ejecución:
                  screen-N: Muestra resumen en pantalla de las primeras N líneas
                  api-N: Procesa las primeras N líneas en la API de Shopify

Ejemplos:
  python main.py productos.xlsx screen-10
  python main.py productos.xlsx api-50
        """)
        sys.exit(1)

    input_file = sys.argv[1]
    mode = sys.argv[2]

    # Validar modo de ejecución
    if not (mode.startswith('screen-') or mode.startswith('api-')):
        logging.error("Error: El modo debe comenzar con 'screen-' o 'api-' seguido de un número")
        sys.exit(1)

    try:
        mode_type, num_lines = mode.split('-')
        num_lines = int(num_lines)
        if num_lines <= 0:
            logging.error("Error: El número de líneas debe ser mayor que 0")
            sys.exit(1)
    except ValueError:
        logging.error("Error: Formato de modo inválido")
        sys.exit(1)

    if not os.path.exists(input_file):
        logging.error(f"Error: El archivo {input_file} no existe")
        sys.exit(1)

    try:
        # Cargar datos
        df = load_data(input_file)
        if df is None:
            logging.error("Error: No se pudo cargar el archivo")
            sys.exit(1)

        # Limitar registros según el número especificado
        if num_lines:
            df = df.head(num_lines)

        # Configurar API si no estamos en modo visualización
        if mode_type == 'api':
            if not setup_shopify_api():
                logging.error("Error: No se pudo establecer conexión con Shopify")
                sys.exit(1)

        # Procesar productos
        process_products(
            df=df,
            display_mode=(mode_type == 'screen')
        )

    except Exception as e:
        logging.error(f"Error en la ejecución: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()