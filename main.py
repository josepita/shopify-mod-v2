#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script principal de sincronización de productos con Shopify
Soporta productos simples y con variantes de talla
Maneja múltiples formatos de archivo (XLS, XLSX, CSV)
"""
import requests 
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
    get_variant_size, extract_measures, extract_diamond_info, extract_stones, extract_zodiac_info,
    extract_shapes_and_letters, extract_medal_figure, extract_medal_type, extract_pendant_type, extract_chain_type 
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
    
    Args:
        product_data: Diccionario con los datos del producto
        product_mapper: Instancia de ProductMapper para mapeo de productos
        location_id: ID de la ubicación en Shopify
        is_update: Indica si es una actualización de producto existente
    
    Returns:
        bool: True si la creación fue exitosa, False en caso contrario
    """
    try:
        # Convertir peso a gramos correctamente
        try:
            weight_in_grams = float(str(product_data.get('weight', 0)).replace(',', '.'))
            if weight_in_grams < 0:
                logging.warning(f"Peso negativo detectado para SKU {product_data['sku']}, ajustando a 0")
                weight_in_grams = 0
        except (ValueError, TypeError):
            logging.warning(f"Error convirtiendo peso para SKU {product_data['sku']}, usando 0")
            weight_in_grams = 0

        print(f"\nProcesando peso para SKU {product_data['sku']}:")
        print(f"- Peso original: {product_data.get('weight', 0)}")
        print(f"- Peso convertido (g): {weight_in_grams}")

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
            'grams': int(weight_in_grams),
            'weight': weight_in_grams,
            'weight_unit': 'g',
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
                #create_product_metafields(new_product.id, product_data['metafields'])
                create_product_metafields_bulk(new_product.id, product_data['metafields'])
            
            # Configurar imágenes
            if product_data.get('images'):
                setup_product_images(new_product.id, product_data['images'])

            # Debuguear producto creado
            updated_product = shopify.Product.find(new_product.id)
            print("\nValores del producto creado:")
            print(f"- ID: {updated_product.id}")
            print(f"- SKU: {updated_product.variants[0].sku}")
            print(f"- Weight (g): {updated_product.variants[0].weight}")
            print(f"- Weight unit: {updated_product.variants[0].weight_unit}")
            print(f"- Grams: {updated_product.variants[0].grams}")
            
            # Verificar coherencia de peso
            if updated_product.variants[0].grams != int(weight_in_grams):
                logging.warning(
                    f"Discrepancia de peso detectada para SKU {product_data['sku']}: "
                    f"esperado {int(weight_in_grams)}g, "
                    f"obtenido {updated_product.variants[0].grams}g"
                )
            
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

    Args:
        product_data: Diccionario con los datos del producto
        shopify_id: ID del producto en Shopify
        product_mapper: Instancia de ProductMapper para el mapeo de productos
        location_id: ID de la ubicación en Shopify
        is_update: Indica si es una actualización de producto existente

    Returns:
        bool: True si la actualización fue exitosa, False en caso contrario
    """
    try:
        existing_product = shopify.Product.find(shopify_id)
        if not existing_product:
            logging.error(f"❌ No se encontró el producto con ID {shopify_id}")
            return False

        # Procesar peso correctamente
        try:
            weight_in_grams = float(str(product_data.get('weight', 0)).replace(',', '.'))
            if weight_in_grams < 0:
                print(f"⚠️ Peso negativo detectado para SKU {product_data['sku']}, ajustando a 0")
                weight_in_grams = 0
        except (ValueError, TypeError):
            weight_in_grams = 0
            print(f"⚠️ Error convirtiendo peso para SKU {product_data['sku']}")

        print(f"\nProcesando peso para SKU {product_data['sku']}:")
        print(f"- Peso original: {product_data.get('weight', 0)}")
        print(f"- Peso exacto (g): {weight_in_grams:.3f}")

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
            variant.weight = weight_in_grams
            variant.weight_unit = 'g'
            variant.cost = product_data.get('cost', 0)
        
        if existing_product.save():
            # Verificar peso guardado
            updated_product = shopify.Product.find(shopify_id)
            print(f"\nValores del producto después de actualizar:")
            print(f"- ID: {updated_product.id}")
            print(f"- SKU: {updated_product.variants[0].sku}")
            print(f"- Peso exacto (g): {updated_product.variants[0].weight:.3f}")
            print(f"- Unidad de peso: {updated_product.variants[0].weight_unit}")

            # Guardar mapeo del producto
            success = product_mapper.save_product_mapping(
                internal_reference=product_data['sku'],
                shopify_product=existing_product,
                is_update=is_update
            )
            
            if not success:
                raise Exception("❌ Error guardando mapeo del producto")
            
            # Configurar inventario
            shopify.InventoryLevel.set(
                location_id=location_id,
                inventory_item_id=existing_product.variants[0].inventory_item_id,
                available=product_data['stock']
            )
            
            # Actualizar metafields
            if product_data.get('metafields'):
                create_product_metafields_bulk(existing_product.id, product_data['metafields'])
            
            # Actualizar imágenes
            if product_data.get('images'):
                for image in existing_product.images:
                    image.destroy()
                setup_product_images(existing_product.id, product_data['images'])
            
            print(f"✅ Producto {product_data['sku']} actualizado con éxito.")
            return True
        else:
            logging.error(f"❌ Error al actualizar producto simple: {existing_product.errors.full_messages()}")
            return False
            
    except Exception as e:
        logging.error(f"❌ Error actualizando producto simple: {str(e)}")
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
            print(f"\nVariante {variant.sku}:")
            print(f"- ID: {variant.id}")
            print(f"- Peso en gramos: {variant.grams}")
            print(f"- Peso (weight): {variant.weight}")
            print(f"- Unidad de peso: {variant.weight_unit}")
            print(f"- Atributos completos: {variant.attributes}")
            
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
            #create_product_metafields(shopify_product_id, product_data['metafields'])
            create_product_metafields_bulk(shopify_product_id, product_data['metafields'])
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

    Args:
        product_data: Diccionario con los datos base del producto
        variants_data: Lista de diccionarios con datos de cada variante
        shopify_id: ID del producto en Shopify
        product_mapper: Instancia de ProductMapper para el mapeo de productos
        location_id: ID de la ubicación en Shopify

    Returns:
        bool: True si la actualización fue exitosa, False en caso contrario
    """
    try:
        print(f"Obteniendo producto de Shopify con ID: {shopify_id}")
        existing_product = shopify.Product.find(shopify_id)
        if not existing_product:
            print(f"No se encontró el producto con ID {shopify_id}")
            return False

        print("Actualizando datos básicos del producto...")
        existing_product.title = product_data['title']
        existing_product.body_html = product_data['body_html']
        existing_product.vendor = product_data['vendor']
        existing_product.product_type = product_data['product_type']
        existing_product.tags = product_data['tags']
        
        print("Actualizando opciones de talla...")
        tallas = [v['size'] for v in variants_data]
        existing_product.options = [{'name': 'Talla', 'values': sorted(list(set(tallas)))}]
        
        if not existing_product.save():
            print(f"Error al actualizar producto base: {existing_product.errors.full_messages()}")
            return False

        print("Actualizando mapeo del producto...")
        success = product_mapper.save_product_mapping(
            internal_reference=product_data['sku'],
            shopify_product=existing_product,
            is_update=True
        )
            
        if not success:
            raise Exception("Error guardando mapeo del producto")

        existing_product.reload()
        
        print("Procesando variantes...")
        existing_variants = {v.sku: v for v in existing_product.variants}
        new_variants = []
        
        for var_data in variants_data:
            variant = None
            is_new_variant = var_data['sku'] not in existing_variants
            
            # Procesar peso correctamente
            try:
                weight_in_grams = float(str(var_data.get('weight', 0)).replace(',', '.'))
                if weight_in_grams < 0:
                    print(f"⚠️ Peso negativo detectado para variante {var_data['sku']}, ajustando a 0")
                    weight_in_grams = 0
            except (ValueError, TypeError):
                weight_in_grams = 0
                print(f"⚠️ Error convirtiendo peso para variante {var_data['sku']}")

            print(f"\nProcesando peso para variante {var_data['sku']}:")
            print(f"- Peso original: {var_data.get('weight', 0)}")
            print(f"- Peso exacto (g): {weight_in_grams:.3f}")
            
            if is_new_variant:
                print(f"Creando nueva variante: {var_data['sku']}")
                variant = shopify.Variant({
                    'product_id': shopify_id,
                    'option1': var_data['size'],
                    'price': var_data['price'],
                    'sku': var_data['sku'],
                    'inventory_management': 'shopify',
                    'inventory_policy': 'deny',
                    'weight': weight_in_grams,
                    'weight_unit': 'g',
                    'cost': var_data.get('cost', 0)
                })
            else:
                print(f"Actualizando variante existente: {var_data['sku']}")
                variant = existing_variants[var_data['sku']]
                variant.option1 = var_data['size']
                variant.price = var_data['price']
                variant.weight = weight_in_grams
                variant.weight_unit = 'g'
                variant.cost = var_data.get('cost', 0)
            
            if not variant.save():
                print(f"Error guardando variante: {variant.errors.full_messages()}")
                continue

            # Verificar peso guardado
            updated_variant = shopify.Variant.find(variant.id)
            print(f"\nValores de la variante {var_data['sku']} después de guardar:")
            print(f"- ID: {updated_variant.id}")
            print(f"- Peso exacto (g): {updated_variant.weight:.3f}")
            print(f"- Unidad de peso: {updated_variant.weight_unit}")
                
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
            create_product_metafields_bulk(shopify_id, product_data['metafields'])
        
        # Actualizar imágenes
        if product_data.get('images'):
            print(f"Actualizando {len(product_data['images'])} imágenes...")
            for image in existing_product.images:
                image.destroy()
            setup_product_images(shopify_id, product_data['images'])
        
        print(f"Producto {product_data['sku']} actualizado exitosamente")
        return True
            
    except Exception as e:
        print(f"Error actualizando producto con variantes: {str(e)}")
        logging.error(f"Error actualizando producto con variantes: {str(e)}")
        return False
            
    except Exception as e:
        print(f"Error actualizando producto con variantes: {str(e)}")
        logging.error(f"Error actualizando producto con variantes: {str(e)}")
        return False

###########################################
# FUNCIONES DE METAFIELDS E IMÁGENES
###########################################

def create_product_metafields_bulk(product_id: int, metafields_data: Dict[str, str]) -> None:
   """
   Crea múltiples metafields para un producto usando GraphQL
   
   Args:
       product_id: ID del producto en Shopify
       metafields_data: Diccionario con los metafields a crear
   """
   # Mapeo de campos
   field_mapping = {
        'alto': {'key': 'alto', 'type': 'number_decimal'},
        'ancho': {'key': 'ancho', 'type': 'number_decimal'},
        'grosor': {'key': 'grosor', 'type': 'number_decimal'},
        'medidas': {'key': 'medidas', 'type': 'single_line_text_field'},
        'largo': {'key': 'largo', 'type': 'number_decimal'},
        'peso': {'key': 'peso', 'type': 'number_decimal'},
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
        'color_diamante': {'key': 'color_diamante', 'type': 'single_line_text_field'},
        'forma_pendientes': {'key': 'forma_pendientes', 'type': 'single_line_text_field'},
        'forma_colgante': {'key': 'forma_colgante', 'type': 'single_line_text_field'},
        'letra': {'key': 'letra', 'type': 'single_line_text_field'},
        'figura_medalla': {'key': 'figura_medalla', 'type': 'single_line_text_field'},
        'tipo_medalla': {'key': 'tipo_medalla', 'type': 'single_line_text_field'},
        'tipo_pendientes': {'key': 'tipo_pendientes', 'type': 'single_line_text_field'},
        'tipo_cadena': {'key': 'tipo_cadena', 'type': 'single_line_text_field'},
        'cadena': {'key': 'cadena', 'type': 'single_line_text_field'}
   }

   if not metafields_data:
       return

   shop_url = SHOPIFY_SHOP_URL.replace('https://', '').replace('http://', '')
   url = f"https://{shop_url}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
   headers = {
       'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
       'Content-Type': 'application/json',
   }

   # Construir inputs para la mutación
   metafield_inputs = []
   for internal_key, value in metafields_data.items():
       if value and str(value).strip():
           field_config = field_mapping.get(internal_key)
           if not field_config:
               logging.warning(f"Campo no mapeado: {internal_key}")
               continue

           formatted_value = value
           if field_config['type'] == 'number_decimal':
               try:
                   formatted_value = str(float(str(value).replace(',', '.')))
               except ValueError:
                   logging.error(f"Error convirtiendo valor a decimal: {value} para campo {internal_key}")
                   continue

           metafield_inputs.append({
               "namespace": "custom",
               "key": field_config['key'],
               "value": formatted_value,
               "type": field_config['type']
           })

   if not metafield_inputs:
       return

   # Construir la mutación GraphQL
   mutation = """
   mutation CreateMetafields($input: [MetafieldsSetInput!]!) {
     metafieldsSet(metafields: $input) {
       metafields {
         key
         value
       }
       userErrors {
         field
         message
       }
     }
   }
   """

   variables = {
       "input": [{
           "ownerId": f"gid://shopify/Product/{product_id}",
           **metafield
       } for metafield in metafield_inputs]
   }

   try:
       response = requests.post(
           url,
           headers=headers,
           json={
               'query': mutation,
               'variables': variables
           }
       )
       
       response.raise_for_status()
       result = response.json()

       if 'errors' in result:
           logging.error(f"Errores creando metafields en bulk: {result['errors']}")
       elif 'data' in result and result['data']['metafieldsSet']['userErrors']:
           logging.error(f"Errores de usuario: {result['data']['metafieldsSet']['userErrors']}")
       else:
           created_fields = len(metafield_inputs)
           logging.info(f"Creados {created_fields} metafields exitosamente para producto {product_id}")
           # Log detallado de los campos creados
           for metafield in metafield_inputs:
               logging.info(f"Metafield creado: {metafield['key']} = {metafield['value']}")

   except Exception as e:
       logging.error(f"Error creando metafields en bulk: {str(e)}")

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
        'peso': {'key': 'peso', 'type': 'number_decimal'},
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
        'color_diamante': {'key': 'color_diamante', 'type': 'single_line_text_field'},
        'forma_pendientes': {'key': 'forma_pendientes', 'type': 'single_line_text_field'},
        'forma_colgante': {'key': 'forma_colgante', 'type': 'single_line_text_field'},
        'letra': {'key': 'letra', 'type': 'single_line_text_field'},
        'figura_medalla': {'key': 'figura_medalla', 'type': 'single_line_text_field'},
        'tipo_medalla': {'key': 'tipo_medalla', 'type': 'single_line_text_field'},
        'tipo_pendientes': {'key': 'tipo_pendientes', 'type': 'single_line_text_field'},
        'tipo_cadena': {'key': 'tipo_cadena', 'type': 'single_line_text_field'},
        'cadena': {'key': 'cadena', 'type': 'single_line_text_field'}

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
                print(f"  - Peso: {product_data['weight']} g")
                print(f"  - Tags: {product_data['tags']}")

                if display_mode:
                    # En modo display, mostrar metafields de manera más legible
                    print("\nMetafields:")
                    for key, value in product_data['metafields'].items():
                        if value:  # Solo mostrar metafields que tienen valor
                            print(f"  - {key}: {value}")
                    
                    print("\nImágenes:")
                    for img in product_data['images']:
                        print(f"  - {img['src']}")

                    if product_info['is_variant_product']:
                        print("\nVariantes:")
                        variants_data = prepare_variants_data(product_info['variants'])
                        for variant in variants_data:
                            print(f"  - SKU: {variant['sku']}")
                            print(f"    Talla: {variant['size']}")
                            print(f"    Precio: {variant['price']} EUR")
                            print(f"    Stock: {variant['stock']}")
                            print(f"    Peso: {variant['weight']} g")

                else:
                    # Aquí va el código existente para modo no-display
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
    description = clean_value(base_row['DESCRIPCION'])
    product_type = clean_value(base_row.get('TIPO', '')).lower()
    
    # Extraer medidas y formas
    measures = extract_measures(description, product_type)
    shapes = extract_shapes_and_letters(description, product_type, description)
    stones_from_desc = extract_stones(description)

    # Preparar los metafields
    metafields = {}
    
    # Extraer información de medallas y colgantes
    metafields.update(extract_medal_figure(description, product_type))
    metafields.update(extract_medal_type(description, product_type))
    metafields.update(extract_pendant_type(description, product_type))
    metafields.update(extract_chain_type(description, product_type))

    # Campos básicos
    if destinatario := clean_value(base_row.get('GENERO', '')):
        metafields['destinatario'] = destinatario.capitalize()
        
    if cierre := clean_value(base_row.get('CIERRE', '')):
        metafields['cierre'] = cierre.capitalize()
        
    if material := get_material(base_row['DESCRIPCION']):
        metafields['material'] = material
        
    if color_oro := clean_value(base_row.get('COLOR ORO', '')):
        metafields['color_oro'] = color_oro.capitalize()
    
    # Campos de piedras - priorizar columnas del CSV
    if piedra := clean_value(base_row.get('PIEDRA', '')):
        metafields['piedra'] = piedra.capitalize()
    elif stones_from_desc:  # Si no hay piedra en el CSV, usar la encontrada en descripción
        metafields.update(stones_from_desc)
        
    if calidad_piedra := clean_value(base_row.get('CALIDAD PIEDRA', '')):
        metafields['calidad_piedra'] = calidad_piedra.capitalize()
    
    # Peso
    if peso := clean_value(base_row.get('PESO G.', '')):
        metafields['peso'] = peso

    # Añadir las medidas y formas extraídas
    metafields.update(measures)
    metafields.update(shapes)

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