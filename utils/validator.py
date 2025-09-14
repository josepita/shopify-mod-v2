from __future__ import annotations

"""
Validador de catálogos CSV/XLS basado en requisitos de columnas y 
coerción numérica mínima (inspirado en shopify-sync CSVProcessor).
"""

from typing import Dict, List, Tuple
import pandas as pd  # type: ignore


REQUIRED_COLUMNS: List[str] = [
    'REFERENCIA', 'DESCRIPCION', 'PRECIO', 'STOCK',
    'CATEGORIA', 'SUBCATEGORIA', 'METAL', 'COLOR ORO',
    'TIPO', 'PESO G.', 'PIEDRA', 'CALIDAD PIEDRA',
    'MEDIDAS', 'CIERRE', 'TALLA', 'GENERO',
    'IMAGEN 1', 'IMAGEN 2', 'IMAGEN 3'
]

NUMERIC_COLUMNS = {
    'PRECIO': {'decimals': True, 'min_value': 0.0},
    'STOCK': {'decimals': False, 'min_value': 0.0},
    'PESO G.': {'decimals': True, 'min_value': 0.0},
}


def validate_catalog_df(df: pd.DataFrame) -> Dict:
    """Valida estructura y algunos indicadores clave del catálogo.

    Returns dict con:
      - ok: bool
      - missing_columns: list[str]
      - stats: dict(total, zero_prices, zero_stock)
      - notes: list[str] (avisos no bloqueantes)
    """
    result = {
        'ok': True,
        'missing_columns': [],
        'stats': {},
        'notes': [],
    }

    # Normalizar cabeceras
    df = df.copy()
    df.columns = df.columns.str.strip()

    # Columnas requeridas
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        result['ok'] = False
        result['missing_columns'] = missing

    # Coerción numérica básica para métricas
    for col, spec in NUMERIC_COLUMNS.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if spec.get('decimals', False):
                df[col] = df[col].round(2)

    total = int(len(df))
    zero_prices_count = int(df[df['PRECIO'].fillna(0) == 0].shape[0]) if 'PRECIO' in df.columns else 0
    zero_stock_count = int(df[df['STOCK'].fillna(0) == 0].shape[0]) if 'STOCK' in df.columns else 0

    result['stats'] = {
        'total': total,
        'zero_prices': {
            'count': zero_prices_count,
            'percent': round((zero_prices_count / total * 100) if total else 0.0, 1)
        },
        'zero_stock': {
            'count': zero_stock_count,
            'percent': round((zero_stock_count / total * 100) if total else 0.0, 1)
        }
    }

    # Avisos
    if result['stats']['zero_stock']['percent'] > 40:
        result['notes'].append('Más del 40% del catálogo tiene stock 0')
    if result['stats']['zero_prices']['count'] > 0:
        result['notes'].append('Existen productos con PRECIO = 0')

    return result

