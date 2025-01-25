import pandas as pd
from datetime import datetime
import os
import csv

def read_csv_with_encoding(file_path):
    """
    Lee el archivo CSV con la configuración específica para este formato
    """
    try:
        df = pd.read_csv(
            file_path,
            encoding='latin-1',    # Para manejar caracteres especiales
            sep=';',               # Separador punto y coma
            quoting=csv.QUOTE_ALL, # Manejo de comillas
            quotechar='"',         # Tipo de comillas usado
            decimal='.'            # Decimal con punto
        )
        return df
    except Exception as e:
        raise ValueError(f"Error al leer el archivo {file_path}: {str(e)}")

def comparar_catalogos(csv1_path, csv2_path):
    """
    Compara dos archivos CSV de catálogo y genera informes de cambios.
    """
    # Leer los CSV
    print("Leyendo archivo reciente...")
    df1 = read_csv_with_encoding(csv1_path)
    print("Leyendo archivo anterior...")
    df2 = read_csv_with_encoding(csv2_path)
    
    # Limpiar y convertir campos numéricos
    for df in [df1, df2]:
        # PRECIO ya viene con punto decimal, solo necesitamos convertirlo a float
        df['PRECIO'] = pd.to_numeric(df['PRECIO'], errors='coerce')
        # Convertir STOCK a entero
        df['STOCK'] = pd.to_numeric(df['STOCK'], errors='coerce').fillna(0).astype(int)
    
    # Obtener fecha actual para los nombres de archivo
    fecha_actual = datetime.now().strftime('%Y%m%d')
    
    # Crear directorio si no existe
    output_dir = '../data/cambios-catalogo/'
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Comparar precios
    print("Comparando precios...")
    df_precios = pd.merge(
        df1[['REFERENCIA', 'NOMBRE', 'PRECIO', 'STOCK']], 
        df2[['REFERENCIA', 'PRECIO']],
        on='REFERENCIA',
        how='inner',
        suffixes=('_nuevo', '_anterior')
    )
    
    df_precios['DIFERENCIA_PRECIO'] = round(df_precios['PRECIO_nuevo'] - df_precios['PRECIO_anterior'], 2)
    cambios_precio = df_precios[abs(df_precios['DIFERENCIA_PRECIO']) > 0.01]
    
    if not cambios_precio.empty:
        cambios_precio = cambios_precio[[
            'REFERENCIA', 'NOMBRE', 'PRECIO_anterior', 'PRECIO_nuevo', 
            'DIFERENCIA_PRECIO', 'STOCK'
        ]]
        archivo_precios = f'{output_dir}cambio-precios-{fecha_actual}.csv'
        cambios_precio.to_csv(
            archivo_precios, 
            index=False, 
            encoding='latin-1', 
            sep=';',
            decimal='.',       # Cambiado a punto para mantener consistencia
            quoting=csv.QUOTE_ALL
        )
        print(f"Archivo de cambios de precios generado: {archivo_precios}")
        print(f"Se encontraron {len(cambios_precio)} cambios de precio")
    else:
        print("No se encontraron cambios en precios")
    
    # 2. Comparar stock
    print("Comparando stock...")
    df_stock = pd.merge(
        df1[['REFERENCIA', 'NOMBRE', 'STOCK']], 
        df2[['REFERENCIA', 'STOCK']],
        on='REFERENCIA',
        how='inner',
        suffixes=('_nuevo', '_anterior')
    )
    
    df_stock['DIFERENCIA_STOCK'] = df_stock['STOCK_nuevo'] - df_stock['STOCK_anterior']
    cambios_stock = df_stock[df_stock['DIFERENCIA_STOCK'] != 0]
    
    if not cambios_stock.empty:
        cambios_stock = cambios_stock[[
            'REFERENCIA', 'NOMBRE', 'STOCK_anterior', 'STOCK_nuevo', 
            'DIFERENCIA_STOCK'
        ]]
        archivo_stock = f'{output_dir}cambio-stock-{fecha_actual}.csv'
        cambios_stock.to_csv(
            archivo_stock, 
            index=False, 
            encoding='latin-1', 
            sep=';',
            quoting=csv.QUOTE_ALL
        )
        print(f"Archivo de cambios de stock generado: {archivo_stock}")
        print(f"Se encontraron {len(cambios_stock)} cambios de stock")
    else:
        print("No se encontraron cambios en stock")
    
    # 3. Referencias dadas de baja
    print("Buscando referencias dadas de baja...")
    referencias_actuales = set(df1['REFERENCIA'])
    referencias_anteriores = set(df2['REFERENCIA'])
    referencias_bajas = referencias_anteriores - referencias_actuales
    
    if referencias_bajas:
        bajas = df2[df2['REFERENCIA'].isin(referencias_bajas)][['REFERENCIA', 'NOMBRE']]
        archivo_bajas = f'{output_dir}bajas-{fecha_actual}.csv'
        bajas.to_csv(
            archivo_bajas, 
            index=False, 
            encoding='latin-1', 
            sep=';',
            quoting=csv.QUOTE_ALL
        )
        print(f"Archivo de bajas generado: {archivo_bajas}")
        print(f"Se encontraron {len(bajas)} referencias dadas de baja")
    else:
        print("No se encontraron referencias dadas de baja")

def main():
    import sys
    
    if len(sys.argv) != 3:
        print("Uso: python script.py <csv1_reciente> <csv2_anterior>")
        sys.exit(1)
        
    csv1_path = sys.argv[1]
    csv2_path = sys.argv[2]
    
    try:
        comparar_catalogos(csv1_path, csv2_path)
        print("\nProceso completado exitosamente.")
    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()