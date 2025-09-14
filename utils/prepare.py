"""
Funciones puras para preparar los datos de productos y variantes
para su inserción/actualización en Shopify. Reutilizadas por CLI y UI web.
"""
from __future__ import annotations

from typing import Dict, List
import pandas as pd

from utils.helpers import (
    clean_value,
    process_tags,
    format_title,
    get_variant_size,
    extract_measures,
    extract_stones,
    extract_shapes_and_letters,
    extract_medal_figure,
    extract_medal_type,
    extract_pendant_type,
    extract_chain_type,
)


def get_material(description: str) -> str:
    """Determina el material basado en la descripción."""
    if isinstance(description, str):
        description = description.upper()
        if description.startswith("18K"):
            return "Oro 18 kilates"
        elif description.startswith("9K"):
            return "Oro 9 kilates"
    return ""


def prepare_images_data(row: pd.Series) -> List[Dict]:
    """Prepara los datos de las imágenes de un producto."""
    images: List[Dict] = []
    for idx, img_col in enumerate(["IMAGEN 1", "IMAGEN 2", "IMAGEN 3"], 1):
        img_src = clean_value(row.get(img_col, ""))
        if img_src:
            if not img_src.startswith(("http://", "https://")):
                img_src = f"https://{img_src}"
            images.append({
                "src": img_src,
                "position": idx,
                "alt": f"{row.get('DESCRIPCION', '')} - Imagen {idx}",
            })
    return images


def prepare_product_data(base_row: pd.Series, base_reference: str) -> Dict:
    """Prepara los datos comunes del producto base para Shopify."""
    description = clean_value(base_row["DESCRIPCION"])
    product_type = clean_value(base_row.get("TIPO", "")).lower()

    # Extraer medidas, formas y piedras
    measures = extract_measures(description, product_type)
    shapes = extract_shapes_and_letters(description, product_type, description)
    stones_from_desc = extract_stones(description)

    # Metafields
    metafields: Dict[str, str] = {}

    # Medallas/colgantes/cadenas
    metafields.update(extract_medal_figure(description, product_type))
    metafields.update(extract_medal_type(description, product_type))
    metafields.update(extract_pendant_type(description, product_type))
    metafields.update(extract_chain_type(description, product_type))

    # Campos básicos
    destinatario = clean_value(base_row.get("GENERO", ""))
    if destinatario:
        metafields["destinatario"] = destinatario.capitalize()

    cierre = clean_value(base_row.get("CIERRE", ""))
    if cierre:
        metafields["cierre"] = cierre.capitalize()

    material = get_material(base_row["DESCRIPCION"]) if "DESCRIPCION" in base_row else ""
    if material:
        metafields["material"] = material

    color_oro = clean_value(base_row.get("COLOR ORO", ""))
    if color_oro:
        metafields["color_oro"] = color_oro.capitalize()

    # Piedras: priorizar columna CSV; si no, usar descripción
    piedra = clean_value(base_row.get("PIEDRA", ""))
    if piedra:
        metafields["piedra"] = piedra.capitalize()
    elif stones_from_desc:
        metafields.update(stones_from_desc)

    calidad_piedra = clean_value(base_row.get("CALIDAD PIEDRA", ""))
    if calidad_piedra:
        metafields["calidad_piedra"] = calidad_piedra.capitalize()

    peso = clean_value(base_row.get("PESO G.", ""))
    if peso:
        metafields["peso"] = peso

    # Añadir medidas y formas
    metafields.update(measures)
    metafields.update(shapes)

    return {
        "title": format_title(base_reference, base_row["DESCRIPCION"]),
        "body_html": description,
        "vendor": "Joyas Armaan",
        "product_type": clean_value(base_row["TIPO"]).capitalize(),
        "tags": process_tags(
            base_row.get("CATEGORIA", ""),
            base_row.get("SUBCATEGORIA", ""),
            base_row.get("TIPO", ""),
        ),
        "sku": base_reference,
        "price": round(float(base_row["PRECIO"]) * 2.2, 2),
        "stock": int(base_row["STOCK"]),
        "weight": clean_value(base_row.get("PESO G.", 0)),
        "cost": clean_value(base_row["PRECIO"]),
        "metafields": metafields,
        "images": prepare_images_data(base_row),
    }


def prepare_variants_data(variants_rows: List[pd.Series]) -> List[Dict]:
    """Prepara los datos de las variantes para Shopify."""
    variants_data: List[Dict] = []
    for row in variants_rows:
        variant_reference = clean_value(row["REFERENCIA"])
        size = get_variant_size(variant_reference)
        if not size:
            continue

        # Peso normalizado (en gramos)
        raw_weight = clean_value(row.get("PESO G.", 0))
        try:
            weight = float(str(raw_weight).replace(",", ".")) if raw_weight else 0.0
        except Exception:
            weight = 0.0

        variants_data.append(
            {
                "size": size,
                "price": round(float(row["PRECIO"]) * 2.2, 2),
                "sku": variant_reference,
                "stock": int(row["STOCK"]),
                "weight": weight,
                "cost": clean_value(row["PRECIO"]),
            }
        )

    return variants_data

