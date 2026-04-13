#!/usr/bin/env python3
"""
Script: Procesamiento de Órdenes de Compra (Inteligencia de Insumos)
Objetivo: Leer Cubo_OC_aspx.xlsx, validar, limpiar y generar 7 JSONs para dashboard
Autor: Claude + Joaco
Fecha: Abril 2026
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging

# ==================== CONFIGURACIÓN LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('procesar_insumos.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
ARCHIVO_ORIGEN = 'Cubo_OC_aspx.xlsx'
CARPETA_SALIDA = './data'
FECHA_MINIMA = pd.Timestamp('2019-10-01')
FECHA_MAXIMA = pd.Timestamp.now()

# Crear carpeta de salida si no existe
Path(CARPETA_SALIDA).mkdir(exist_ok=True)

# ==================== FUNCIONES AUXILIARES ====================

def cargar_archivo(archivo):
    """Carga el archivo Excel con manejo de errores"""
    try:
        logger.info(f"Cargando archivo: {archivo}")
        df = pd.read_excel(archivo, sheet_name='Cubo_OC.aspx', skiprows=1)
        logger.info(f"✓ Archivo cargado exitosamente: {len(df)} registros")
        return df
    except Exception as e:
        logger.error(f"Error al cargar archivo: {e}")
        raise

def normalizar_proveedores(df):
    """Normaliza nombres de proveedores (trim, mayúsculas)"""
    df['Proveedor'] = df['Proveedor'].str.strip().str.upper()
    return df

def convertir_a_usd(row):
    """Convierte Total a USD según Moneda Carga y Cotización"""
    if pd.isna(row['Moneda Carga']):
        return row['Total']
    
    if row['Moneda Carga'].upper() == 'DOLARES':
        return row['Total']
    elif row['Moneda Carga'].upper() == 'PESOS':
        if pd.notna(row['Cotización']) and row['Cotización'] > 0:
            return row['Total'] / row['Cotización']
        else:
            logger.warning(f"Cotización faltante para conversión ARS→USD. ID: {row['ID_Comprobante']}")
            return row['Total']
    else:
        logger.warning(f"Moneda desconocida: {row['Moneda Carga']}")
        return row['Total']

def validar_y_limpiar(df):
    """Valida y limpia los datos"""
    logger.info("\n=== VALIDACIÓN Y LIMPIEZA ===")
    
    registros_iniciales = len(df)
    
    # 1. Eliminar registros con Cantidad = 0 o Total = 0
    antes = len(df)
    df = df[(df['Cantidad'] > 0) & (df['Total'] > 0)]
    logger.info(f"Eliminar Cantidad=0 o Total=0: {antes - len(df)} registros removidos")
    
    # 2. Validar fechas
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
    antes = len(df)
    df = df[(df['Fecha'] >= FECHA_MINIMA) & (df['Fecha'] <= FECHA_MAXIMA)]
    logger.info(f"Validar fechas (2019-10-01 a hoy): {antes - len(df)} registros fuera de rango")
    
    # 3. Convertir a USD
    df['Total_USD'] = df.apply(convertir_a_usd, axis=1)
    
    # 4. Normalizar proveedores
    df = normalizar_proveedores(df)
    
    # 5. Detectar outliers en precio (precio > media + 3σ)
    df['Precio Unitario'] = pd.to_numeric(df['Precio Unitario'], errors='coerce')
    media_precio = df['Precio Unitario'].mean()
    sigma_precio = df['Precio Unitario'].std()
    threshold_alto = media_precio + (3 * sigma_precio)
    threshold_bajo = media_precio - (3 * sigma_precio)
    
    outliers = df[(df['Precio Unitario'] > threshold_alto) | (df['Precio Unitario'] < threshold_bajo)]
    if len(outliers) > 0:
        logger.warning(f"⚠ {len(outliers)} registros detectados como OUTLIERS en precio:")
        for _, row in outliers.head(5).iterrows():
            logger.warning(f"   {row['Insumo']} - Proveedor: {row['Proveedor']} - Precio: ${row['Precio Unitario']:.2f}")
        if len(outliers) > 5:
            logger.warning(f"   ... y {len(outliers) - 5} más")
    
    # 6. Validar consistencia: Total ≈ Cantidad × Precio (tolerancia 5%)
    df['consistencia'] = abs(df['Total'] - (df['Cantidad'] * df['Precio Unitario'])) / (df['Total'] + 1)
    inconsistentes = df[df['consistencia'] > 0.05]
    if len(inconsistentes) > 0:
        logger.warning(f"⚠ {len(inconsistentes)} registros con inconsistencia en Total vs Cantidad×Precio")
    
    registros_finales = len(df)
    logger.info(f"✓ Validación completada: {registros_iniciales} → {registros_finales} registros ({100*registros_finales/registros_iniciales:.1f}%)")
    
    return df

def generar_compras_limpio(df):
    """Genera compras_limpio.json"""
    logger.info("\n=== GENERANDO compras_limpio.json ===")
    
    registros = []
    for idx, row in df.iterrows():
        registro = {
            "id": f"{int(row['ID_Comprobante'])}_{int(row['ID_Items_Compra']) if pd.notna(row['ID_Items_Compra']) else idx}",
            "fecha": row['Fecha'].strftime('%Y-%m-%d') if pd.notna(row['Fecha']) else None,
            "campaña": row['Campañas'],
            "proveedor": row['Proveedor'],
            "insumo": row['Insumo'],
            "tipo_insumo": row['Tipo de insumo'],
            "cantidad": float(row['Cantidad']),
            "unidad": row['Unidad de medida'],
            "precio_unitario": float(row['Precio Unitario']),
            "total_usd": float(row['Total_USD']),
            "cotización": float(row['Cotización']) if pd.notna(row['Cotización']) else None,
            "referencia": row['Referencia'],
            "tipo_comprobante": row['Tipo Comprobante'],
            "estado_compra": row['Estado Compra'],
            "estado_stock": row['Estado Stock'],
            "punto_stock": row['Punto de stock'] if pd.notna(row['Punto de stock']) else None,
            "observaciones": row['Observaciones'] if pd.notna(row['Observaciones']) else None
        }
        registros.append(registro)
    
    salida = {
        "registros": registros,
        "meta": {
            "fecha_actualización": datetime.now().isoformat(),
            "total_registros": len(registros),
            "rango_fechas": [
                df['Fecha'].min().strftime('%Y-%m-%d'),
                df['Fecha'].max().strftime('%Y-%m-%d')
            ]
        }
    }
    
    guardar_json('compras_limpio.json', salida)
    logger.info(f"✓ {len(registros)} registros guardados")
    return df

def generar_gasto_por_insumo_campaña(df):
    """Genera kpi_gasto_por_insumo_campaña.json"""
    logger.info("\n=== GENERANDO kpi_gasto_por_insumo_campaña.json ===")
    
    # Crear pivot table
    pivot = df.pivot_table(
        values='Total_USD',
        index=['Insumo', 'Tipo de insumo'],
        columns='Campañas',
        aggfunc='sum',
        fill_value=0
    )
    
    # Convertir a estructura esperada
    data = []
    for (insumo, tipo_insumo), row in pivot.iterrows():
        registro = {
            "insumo": insumo,
            "tipo_insumo": tipo_insumo
        }
        
        # Agregar campañas
        for campaña in sorted(df['Campañas'].unique()):
            registro[campaña] = float(row.get(campaña, 0))
        
        # Total histórico
        registro["total_histórico"] = sum(float(row.get(c, 0)) for c in df['Campañas'].unique())
        
        data.append(registro)
    
    # Ordenar por total histórico
    data.sort(key=lambda x: x['total_histórico'], reverse=True)
    
    salida = {
        "data": data,
        "meta": {
            "fecha_actualización": datetime.now().isoformat(),
            "total_insumos": len(data),
            "campañas": sorted(df['Campañas'].unique().tolist())
        }
    }
    
    guardar_json('kpi_gasto_por_insumo_campaña.json', salida)
    logger.info(f"✓ {len(data)} insumos procesados")

def generar_precio_promedio(df):
    """Genera kpi_precio_promedio.json"""
    logger.info("\n=== GENERANDO kpi_precio_promedio.json ===")
    
    # Obtener campañas ordenadas
    campañas = sorted(df['Campañas'].unique())
    
    # Agrupar por insumo y campaña
    grupo = df.groupby(['Insumo', 'Tipo de insumo', 'Campañas'])['Precio Unitario'].mean().reset_index()
    
    data = []
    insumos_unicos = grupo[['Insumo', 'Tipo de insumo']].drop_duplicates()
    
    for _, row_insumo in insumos_unicos.iterrows():
        insumo = row_insumo['Insumo']
        tipo_insumo = row_insumo['Tipo de insumo']
        
        # Filtrar datos para este insumo
        datos_insumo = grupo[(grupo['Insumo'] == insumo) & (grupo['Tipo de insumo'] == tipo_insumo)]
        
        # Obtener última campaña y anterior
        campañas_presentes = datos_insumo['Campañas'].unique()
        
        if len(campañas_presentes) >= 1:
            campaña_actual = campañas_presentes[-1]  # Última
            precio_actual = datos_insumo[datos_insumo['Campañas'] == campaña_actual]['Precio Unitario'].values[0]
            
            if len(campañas_presentes) >= 2:
                campaña_anterior = campañas_presentes[-2]
                precio_anterior = datos_insumo[datos_insumo['Campañas'] == campaña_anterior]['Precio Unitario'].values[0]
                variación_pct = ((precio_actual - precio_anterior) / precio_anterior) * 100
            else:
                campaña_anterior = None
                precio_anterior = precio_actual
                variación_pct = 0
            
            tendencia = "↑" if variación_pct > 0 else ("↓" if variación_pct < 0 else "→")
            color = "rojo" if variación_pct > 5 else ("verde" if variación_pct < -5 else "gris")
            
            registro = {
                "insumo": insumo,
                "tipo_insumo": tipo_insumo,
                "precio_promedio_actual": float(round(precio_actual, 2)),
                "campaña_actual": campaña_actual,
                "precio_promedio_anterior": float(round(precio_anterior, 2)) if campaña_anterior else None,
                "campaña_anterior": campaña_anterior,
                "variación_porcentaje": float(round(variación_pct, 2)),
                "tendencia": tendencia,
                "color": color
            }
            data.append(registro)
    
    salida = {
        "data": data,
        "meta": {
            "fecha_actualización": datetime.now().isoformat(),
            "total_insumos": len(data)
        }
    }
    
    guardar_json('kpi_precio_promedio.json', salida)
    logger.info(f"✓ {len(data)} insumos con análisis de precio")

def generar_variación_campañas(df):
    """Genera kpi_variación_campañas.json"""
    logger.info("\n=== GENERANDO kpi_variación_campañas.json ===")
    
    # Top 10 insumos por gasto histórico
    top_10 = df.groupby('Insumo')['Total_USD'].sum().nlargest(10).index.tolist()
    
    data = []
    for insumo in top_10:
        df_insumo = df[df['Insumo'] == insumo]
        tipo_insumo = df_insumo['Tipo de insumo'].iloc[0]
        
        campañas_data = {}
        for campaña in sorted(df_insumo['Campañas'].unique()):
            df_campaña = df_insumo[df_insumo['Campañas'] == campaña]
            gasto = float(df_campaña['Total_USD'].sum())
            volumen = float(df_campaña['Cantidad'].sum())
            precio_promedio = float(df_campaña['Precio Unitario'].mean())
            
            campañas_data[campaña] = {
                "gasto": gasto,
                "volumen": volumen,
                "precio_promedio": round(precio_promedio, 2)
            }
        
        # Calcular variación 24/25 vs 25/26
        variación_precio = None
        variación_volumen = None
        
        if '24/25' in campañas_data and '25/26' in campañas_data:
            p_24_25 = campañas_data['24/25']['precio_promedio']
            p_25_26 = campañas_data['25/26']['precio_promedio']
            if p_24_25 > 0:
                variación_precio = ((p_25_26 - p_24_25) / p_24_25) * 100
            
            v_24_25 = campañas_data['24/25']['volumen']
            v_25_26 = campañas_data['25/26']['volumen']
            if v_24_25 > 0:
                variación_volumen = ((v_25_26 - v_24_25) / v_24_25) * 100
        
        registro = {
            "insumo": insumo,
            "tipo_insumo": tipo_insumo,
            "campañas": campañas_data,
            "variación_precio_24_25_vs_25_26": float(round(variación_precio, 2)) if variación_precio else None,
            "variación_volumen_24_25_vs_25_26": float(round(variación_volumen, 2)) if variación_volumen else None
        }
        data.append(registro)
    
    salida = {
        "top_10_insumos": top_10,
        "data": data,
        "meta": {
            "fecha_actualización": datetime.now().isoformat()
        }
    }
    
    guardar_json('kpi_variación_campañas.json', salida)
    logger.info(f"✓ Top 10 insumos procesados con variación entre campañas")

def generar_proveedor_precio(df):
    """Genera kpi_proveedor_precio.json"""
    logger.info("\n=== GENERANDO kpi_proveedor_precio.json ===")
    
    # Top 10 insumos por gasto
    top_10 = df.groupby('Insumo')['Total_USD'].sum().nlargest(10).index.tolist()
    
    top_10_data = []
    for insumo in top_10:
        df_insumo = df[df['Insumo'] == insumo]
        tipo_insumo = df_insumo['Tipo de insumo'].iloc[0]
        
        # Agrupar por proveedor
        proveedores = df_insumo.groupby('Proveedor').agg({
            'Precio Unitario': 'mean',
            'ID_Comprobante': 'count',
            'Total_USD': 'sum'
        }).reset_index()
        
        proveedores.columns = ['nombre', 'precio_promedio', 'órdenes', 'gasto_total']
        proveedores = proveedores.sort_values('precio_promedio')
        proveedores['ranking'] = range(1, len(proveedores) + 1)
        
        # Calcular ahorro potencial
        precio_min = proveedores['precio_promedio'].min()
        precio_max = proveedores['precio_promedio'].max()
        ahorro_potencial_pct = ((precio_max - precio_min) / precio_max) * 100 if precio_max > 0 else 0
        
        # Usar el volumen promedio estimado para calcular ahorro en USD
        volumen_promedio = df_insumo['Cantidad'].sum() / len(df_insumo.groupby('ID_Comprobante'))
        ahorro_potencial_usd = (precio_max - precio_min) * volumen_promedio
        
        proveedores_list = proveedores[['nombre', 'precio_promedio', 'órdenes', 'gasto_total', 'ranking']].to_dict('records')
        
        # Convertir a float
        for p in proveedores_list:
            p['precio_promedio'] = float(round(p['precio_promedio'], 2))
            p['órdenes'] = int(p['órdenes'])
            p['gasto_total'] = float(round(p['gasto_total'], 2))
        
        registro_insumo = {
            "insumo": insumo,
            "tipo_insumo": tipo_insumo,
            "proveedores": proveedores_list,
            "ahorro_potencial_porcentaje": float(round(ahorro_potencial_pct, 2)),
            "ahorro_potencial_usd": float(round(ahorro_potencial_usd, 2))
        }
        top_10_data.append(registro_insumo)
    
    salida = {
        "top_10_insumos": top_10,
        "data": top_10_data,
        "meta": {
            "fecha_actualización": datetime.now().isoformat()
        }
    }
    
    guardar_json('kpi_proveedor_precio.json', salida)
    logger.info(f"✓ Top 10 insumos con análisis de proveedores")

def generar_panorama(df):
    """Genera kpi_panorama.json"""
    logger.info("\n=== GENERANDO kpi_panorama.json ===")
    
    # Gasto por campaña
    gasto_por_campaña = df.groupby('Campañas')['Total_USD'].sum().to_dict()
    gasto_por_campaña = {k: float(round(v, 2)) for k, v in gasto_por_campaña.items()}
    
    # Gasto por tipo de insumo
    gasto_por_tipo = df.groupby('Tipo de insumo')['Total_USD'].sum().sort_values(ascending=False).to_dict()
    gasto_por_tipo = {k: float(round(v, 2)) for k, v in gasto_por_tipo.items()}
    
    # Top 5 proveedores
    top_5_proveedores = df.groupby('Proveedor')['Total_USD'].sum().nlargest(5)
    gasto_total = top_5_proveedores.sum()
    
    top_5_list = []
    for proveedor, gasto in top_5_proveedores.items():
        top_5_list.append({
            "nombre": proveedor,
            "gasto": float(round(gasto, 2)),
            "porcentaje": float(round((gasto / gasto_total) * 100, 1))
        })
    
    # Concentración top 3
    top_3_gasto = top_5_proveedores.head(3).sum()
    concentración_top_3 = (top_3_gasto / gasto_total) * 100
    
    # Promedio mensual
    promedio_mensual = gasto_total / ((df['Fecha'].max() - df['Fecha'].min()).days / 30)
    
    salida = {
        "gasto_total_por_campaña": gasto_por_campaña,
        "gasto_por_tipo_insumo": gasto_por_tipo,
        "top_5_proveedores": top_5_list,
        "concentración_top_3": float(round(concentración_top_3, 1)),
        "promedio_mensual": float(round(promedio_mensual, 2)),
        "gasto_total_histórico": float(round(gasto_total, 2)),
        "meta": {
            "fecha_actualización": datetime.now().isoformat(),
            "total_registros": len(df)
        }
    }
    
    guardar_json('kpi_panorama.json', salida)
    logger.info(f"✓ Panorama general calculado")

def generar_evolución_mensual(df):
    """Genera evolución_mensual.json (últimos 24 meses)"""
    logger.info("\n=== GENERANDO evolución_mensual.json ===")
    
    # Últimos 24 meses
    fecha_limite = df['Fecha'].max() - timedelta(days=730)
    df_24m = df[df['Fecha'] >= fecha_limite].copy()
    
    # Agrupar por mes
    df_24m['mes'] = df_24m['Fecha'].dt.to_period('M')
    evolución = df_24m.groupby('mes').agg({
        'Total_USD': 'sum',
        'ID_Comprobante': 'count'
    }).reset_index()
    
    evolución.columns = ['mes', 'gasto_usd', 'órdenes']
    evolución['mes'] = evolución['mes'].astype(str)
    
    data = []
    for _, row in evolución.iterrows():
        data.append({
            "mes": row['mes'],
            "gasto_usd": float(round(row['gasto_usd'], 2)),
            "órdenes": int(row['órdenes'])
        })
    
    salida = {
        "data": data,
        "meta": {
            "fecha_actualización": datetime.now().isoformat(),
            "rango": "últimos 24 meses",
            "meses_procesados": len(data)
        }
    }
    
    guardar_json('evolución_mensual.json', salida)
    logger.info(f"✓ {len(data)} meses procesados")

def guardar_json(nombre, datos):
    """Guarda datos en archivo JSON con formato bonito"""
    ruta = os.path.join(CARPETA_SALIDA, nombre)
    try:
        with open(ruta, 'w', encoding='utf-8') as f:
            json.dump(datos, f, indent=2, ensure_ascii=False)
        logger.info(f"   Guardado: {ruta}")
    except Exception as e:
        logger.error(f"Error guardando {nombre}: {e}")
        raise

# ==================== MAIN ====================

def main():
    logger.info("="*60)
    logger.info("INICIO: Procesamiento de Órdenes de Compra de Insumos")
    logger.info("="*60)
    
    try:
        # 1. Cargar archivo
        df = cargar_archivo(ARCHIVO_ORIGEN)
        
        # 2. Validar y limpiar
        df = validar_y_limpiar(df)
        
        # 3. Generar JSONs
        generar_compras_limpio(df)
        generar_gasto_por_insumo_campaña(df)
        generar_precio_promedio(df)
        generar_variación_campañas(df)
        generar_proveedor_precio(df)
        generar_panorama(df)
        generar_evolución_mensual(df)
        
        logger.info("\n" + "="*60)
        logger.info("✓ COMPLETADO: Todos los JSONs generados exitosamente")
        logger.info("="*60)
        logger.info(f"Archivos guardados en: {os.path.abspath(CARPETA_SALIDA)}")
        
    except Exception as e:
        logger.error(f"\n✗ ERROR: {e}")
        raise

if __name__ == "__main__":
    main()
