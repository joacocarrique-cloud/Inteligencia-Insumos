#!/usr/bin/env python3
"""
Actualizar Dashboard de Inteligencia de Insumos
================================================
Lee Cubo_OC_aspx.xlsx desde OneDrive, procesa los datos y genera
un HTML self-contained con todos los datos embebidos.

Uso:
  python actualizar_dashboard.py
  python actualizar_dashboard.py --origen "/ruta/a/Cubo_OC_aspx.xlsx"
  python actualizar_dashboard.py --onedrive "/ruta/a/carpeta/OneDrive"

Requisitos:
  pip install pandas openpyxl
"""

import pandas as pd
import numpy as np
import json
import os
import sys
import argparse
import platform
from datetime import datetime, timedelta
from pathlib import Path
import logging

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s │ %(levelname)-7s │ %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler('actualizar_dashboard.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================
NOMBRE_ARCHIVO = 'Cubo_Oc.aspx'
NOMBRE_HOJA = 'Cubo_OC.aspx'
FECHA_MINIMA = pd.Timestamp('2019-10-01')
RUTA_ONEDRIVE_DEFAULT = r'C:\Users\Joaquin\OneDrive - ESPARTINA S.A\DocumentacionEspartina\COMERCIAL\Tablas PBI'
HTML_TEMPLATE_MARKER = '/* __DATA_PLACEHOLDER__ */'


def detectar_onedrive():
    """Intenta encontrar la carpeta de OneDrive automáticamente."""
    sistema = platform.system()
    home = Path.home()

    candidatos = []
    if sistema == 'Windows':
        candidatos = [
            Path(RUTA_ONEDRIVE_DEFAULT).parent.parent.parent.parent,  # OneDrive - ESPARTINA S.A
            home / 'OneDrive - ESPARTINA S.A',
            home / 'OneDrive',
            Path(os.environ.get('OneDrive', '')) if os.environ.get('OneDrive') else None,
            Path(os.environ.get('OneDriveCommercial', '')) if os.environ.get('OneDriveCommercial') else None,
        ]
    elif sistema == 'Darwin':  # macOS
        candidatos = [
            home / 'Library' / 'CloudStorage' / 'OneDrive-Personal',
            home / 'Library' / 'CloudStorage' / 'OneDrive-Espartina',
            home / 'OneDrive',
            home / 'OneDrive - Espartina',
        ]
    elif sistema == 'Linux':
        candidatos = [
            home / 'OneDrive',
            home / '.local' / 'share' / 'onedrive',
        ]

    # Filtrar None y buscar el archivo
    for carpeta in [c for c in candidatos if c]:
        if carpeta.exists():
            # Buscar el archivo recursivamente (máx 3 niveles)
            for depth in range(4):
                for match in carpeta.glob('/'.join(['*'] * depth + [NOMBRE_ARCHIVO])):
                    return match

    return None


def buscar_archivo(args):
    """Determina la ruta del archivo Excel."""
    if args.origen:
        ruta = Path(args.origen)
        if ruta.exists():
            return ruta
        log.error(f"Archivo no encontrado: {ruta}")
        sys.exit(1)

    if args.onedrive:
        carpeta = Path(args.onedrive)
        if not carpeta.exists():
            log.error(f"Carpeta OneDrive no existe: {carpeta}")
            sys.exit(1)
        archivo = carpeta / NOMBRE_ARCHIVO
        if archivo.exists():
            return archivo
        # Buscar recursivamente
        for match in carpeta.rglob(NOMBRE_ARCHIVO):
            return match
        log.error(f"No se encontró {NOMBRE_ARCHIVO} en {carpeta}")
        sys.exit(1)

    # Ruta por defecto (Espartina OneDrive)
    ruta_default = Path(RUTA_ONEDRIVE_DEFAULT) / NOMBRE_ARCHIVO
    if ruta_default.exists():
        return ruta_default

    # Auto-detectar en rutas comunes de OneDrive
    log.info("Buscando archivo en OneDrive automáticamente...")
    ruta = detectar_onedrive()
    if ruta:
        return ruta

    # Último intento: directorio actual
    local = Path('.') / NOMBRE_ARCHIVO
    if local.exists():
        return local

    log.error(f"No se encontró {NOMBRE_ARCHIVO}.")
    log.error(f"  Ruta esperada: {ruta_default}")
    log.error(f"  Usá --origen para especificar la ruta manualmente.")
    sys.exit(1)


# ==================== PROCESAMIENTO ====================

def cargar_y_limpiar(ruta):
    """Carga el Excel, valida y limpia los datos."""
    log.info(f"Cargando: {ruta}")
    # El archivo .aspx es un Excel exportado desde el ERP — se lee igual con openpyxl
    try:
        df = pd.read_excel(ruta, sheet_name=NOMBRE_HOJA, skiprows=1, engine='openpyxl')
    except Exception:
        # Si falla con openpyxl, intentar sin especificar engine
        df = pd.read_excel(ruta, sheet_name=NOMBRE_HOJA, skiprows=1)
    log.info(f"  {len(df)} registros cargados")

    n0 = len(df)

    # Tipos numéricos
    df['Cantidad'] = pd.to_numeric(df['Cantidad'], errors='coerce').fillna(0)
    df['Total'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
    df['Precio Unitario'] = pd.to_numeric(df['Precio Unitario'], errors='coerce').fillna(0)
    df['Cotización'] = pd.to_numeric(df['Cotización'], errors='coerce')

    # Eliminar cantidad=0 o total=0
    df = df[(df['Cantidad'] > 0) & (df['Total'] > 0)]
    log.info(f"  Eliminados {n0 - len(df)} registros con Cantidad=0 o Total=0")

    # Fechas
    df['Fecha'] = pd.to_datetime(df['Fecha'], errors='coerce')
    hoy = pd.Timestamp.now()
    df = df[(df['Fecha'] >= FECHA_MINIMA) & (df['Fecha'] <= hoy)]

    # USD
    def to_usd(row):
        moneda = str(row.get('Moneda Carga', '')).upper()
        if 'PESO' in moneda and pd.notna(row['Cotización']) and row['Cotización'] > 0:
            return row['Total'] / row['Cotización']
        return row['Total']

    df['Total_USD'] = df.apply(to_usd, axis=1)

    # Normalizar proveedores
    df['Proveedor'] = df['Proveedor'].astype(str).str.strip().str.upper()
    df['Insumo'] = df['Insumo'].astype(str).str.strip()
    df['Tipo de insumo'] = df['Tipo de insumo'].astype(str).str.strip()
    df['Campañas'] = df['Campañas'].astype(str).str.strip()

    # Outliers (solo log, no eliminar)
    media = df['Precio Unitario'].mean()
    sigma = df['Precio Unitario'].std()
    outliers = df[df['Precio Unitario'] > media + 3 * sigma]
    if len(outliers) > 0:
        log.warning(f"  ⚠ {len(outliers)} outliers de precio detectados (no eliminados)")

    log.info(f"  ✓ {len(df)} registros válidos listos")
    return df


def generar_datos(df):
    """Genera todos los datasets para el dashboard."""

    # ---------- PANORAMA ----------
    gasto_x_camp = {k: round(v, 2) for k, v in df.groupby('Campañas')['Total_USD'].sum().items()}
    gasto_x_tipo = {k: round(v, 2) for k, v in df.groupby('Tipo de insumo')['Total_USD'].sum().sort_values(ascending=False).items()}

    top5_prov_series = df.groupby('Proveedor')['Total_USD'].sum().nlargest(5)
    gasto_total = df['Total_USD'].sum()
    top5_prov = [{"nombre": n, "gasto": round(g, 2), "porcentaje": round(g / gasto_total * 100, 1)} for n, g in top5_prov_series.items()]
    conc_top3 = round(sum(p['gasto'] for p in top5_prov[:3]) / gasto_total * 100, 1)

    meses_rango = max(1, (df['Fecha'].max() - df['Fecha'].min()).days / 30)
    prom_mensual = round(gasto_total / meses_rango, 2)

    panorama = {
        "gasto_total_por_campaña": gasto_x_camp,
        "gasto_por_tipo_insumo": gasto_x_tipo,
        "top_5_proveedores": top5_prov,
        "concentración_top_3": conc_top3,
        "promedio_mensual": prom_mensual,
        "gasto_total_histórico": round(gasto_total, 2),
        "meta": {"fecha_actualización": datetime.now().isoformat(), "total_registros": len(df)}
    }

    # ---------- EVOLUCIÓN MENSUAL ----------
    fecha_lim = df['Fecha'].max() - timedelta(days=730)
    df_24m = df[df['Fecha'] >= fecha_lim].copy()
    df_24m['mes'] = df_24m['Fecha'].dt.to_period('M')
    evol_grp = df_24m.groupby('mes').agg({'Total_USD': 'sum', 'ID_Comprobante': 'count'}).reset_index()
    evol_grp.columns = ['mes', 'gasto_usd', 'órdenes']
    evolucion = {
        "data": [{"mes": str(r['mes']), "gasto_usd": round(r['gasto_usd'], 2), "órdenes": int(r['órdenes'])} for _, r in evol_grp.iterrows()],
        "meta": {"fecha_actualización": datetime.now().isoformat(), "meses_procesados": len(evol_grp)}
    }

    # ---------- GASTO POR INSUMO × CAMPAÑA ----------
    pivot = df.pivot_table(values='Total_USD', index=['Insumo', 'Tipo de insumo'], columns='Campañas', aggfunc='sum', fill_value=0)
    camps_sorted = sorted(df['Campañas'].unique())
    gasto_insumo = []
    for (ins, tipo), row in pivot.iterrows():
        r = {"insumo": ins, "tipo_insumo": tipo}
        for c in camps_sorted:
            r[c] = round(float(row.get(c, 0)), 2)
        r["total_histórico"] = round(sum(r.get(c, 0) for c in camps_sorted), 2)
        gasto_insumo.append(r)
    gasto_insumo.sort(key=lambda x: x['total_histórico'], reverse=True)

    # ---------- PRECIO PROMEDIO ----------
    precio_grp = df.groupby(['Insumo', 'Tipo de insumo', 'Campañas'])['Precio Unitario'].mean().reset_index()
    precio_data = []
    for (ins, tipo), grp in precio_grp.groupby(['Insumo', 'Tipo de insumo']):
        camps_presentes = sorted(grp['Campañas'].values)
        c_actual = camps_presentes[-1]
        p_actual = float(grp[grp['Campañas'] == c_actual]['Precio Unitario'].values[0])
        if len(camps_presentes) >= 2:
            c_ant = camps_presentes[-2]
            p_ant = float(grp[grp['Campañas'] == c_ant]['Precio Unitario'].values[0])
            var_pct = ((p_actual - p_ant) / p_ant) * 100 if p_ant else 0
        else:
            c_ant, p_ant, var_pct = None, p_actual, 0

        precio_data.append({
            "insumo": ins, "tipo_insumo": tipo,
            "precio_promedio_actual": round(p_actual, 2),
            "precio_promedio_anterior": round(p_ant, 2) if c_ant else None,
            "variación_porcentaje": round(var_pct, 2),
            "tendencia": "↑" if var_pct > 0 else ("↓" if var_pct < 0 else "→"),
            "color": "rojo" if var_pct > 5 else ("verde" if var_pct < -5 else "gris")
        })

    # Merge gasto + precio → insumos unificado
    precio_map = {r['insumo']: r for r in precio_data}
    insumos_merged = []
    for g in gasto_insumo:
        p = precio_map.get(g['insumo'], {})
        insumos_merged.append({
            **g,
            "precio_promedio_actual": p.get('precio_promedio_actual'),
            "precio_promedio_anterior": p.get('precio_promedio_anterior'),
            "variación_porcentaje": p.get('variación_porcentaje', 0),
            "tendencia": p.get('tendencia', '→'),
            "color": p.get('color', 'gris')
        })

    # ---------- VARIACIÓN CAMPAÑAS TOP 10 ----------
    top10_names = [x['insumo'] for x in gasto_insumo[:10]]
    variacion_data = []
    for ins in top10_names:
        df_ins = df[df['Insumo'] == ins]
        tipo = df_ins['Tipo de insumo'].iloc[0]
        camps_d = {}
        for c in sorted(df_ins['Campañas'].unique()):
            dc = df_ins[df_ins['Campañas'] == c]
            camps_d[c] = {"gasto": round(float(dc['Total_USD'].sum()), 2), "volumen": round(float(dc['Cantidad'].sum()), 2), "precio_promedio": round(float(dc['Precio Unitario'].mean()), 2)}
        vp, vv = 0, 0
        if '24/25' in camps_d and '25/26' in camps_d:
            p1, p2 = camps_d['24/25']['precio_promedio'], camps_d['25/26']['precio_promedio']
            v1, v2 = camps_d['24/25']['volumen'], camps_d['25/26']['volumen']
            if p1: vp = round(((p2 - p1) / p1) * 100, 2)
            if v1: vv = round(((v2 - v1) / v1) * 100, 2)
        variacion_data.append({"insumo": ins, "tipo_insumo": tipo, "campañas": camps_d, "variación_precio_24_25_vs_25_26": vp, "variación_volumen_24_25_vs_25_26": vv})

    variacion = {"top_10_insumos": top10_names, "data": variacion_data, "meta": {"fecha_actualización": datetime.now().isoformat()}}

    # ---------- PROVEEDOR PRECIO ----------
    prov_data = []
    for ins in top10_names:
        df_ins = df[df['Insumo'] == ins]
        tipo = df_ins['Tipo de insumo'].iloc[0]
        pg = df_ins.groupby('Proveedor').agg({'Precio Unitario': 'mean', 'ID_Comprobante': 'count', 'Total_USD': 'sum'}).reset_index()
        pg.columns = ['nombre', 'precio_promedio', 'órdenes', 'gasto_total']
        pg = pg.sort_values('precio_promedio')
        pg['ranking'] = range(1, len(pg) + 1)
        provs = [{"nombre": r['nombre'], "precio_promedio": round(r['precio_promedio'], 2), "órdenes": int(r['órdenes']), "gasto_total": round(r['gasto_total'], 2), "ranking": int(r['ranking'])} for _, r in pg.iterrows()]
        pmin, pmax = pg['precio_promedio'].min(), pg['precio_promedio'].max()
        ahorro_pct = round(((pmax - pmin) / pmax) * 100, 2) if pmax > 0 else 0
        vol_prom = df_ins['Cantidad'].sum() / max(1, df_ins['ID_Comprobante'].nunique())
        ahorro_usd = round((pmax - pmin) * vol_prom, 2)
        prov_data.append({"insumo": ins, "tipo_insumo": tipo, "proveedores": provs, "ahorro_potencial_porcentaje": ahorro_pct, "ahorro_potencial_usd": ahorro_usd})

    proveedor = {"top_10_insumos": top10_names, "data": prov_data, "meta": {"fecha_actualización": datetime.now().isoformat()}}

    # ---------- COMPRAS SLIM (para detalle) ----------
    compras_slim = []
    for _, r in df.iterrows():
        compras_slim.append([
            r['Fecha'].strftime('%Y-%m-%d') if pd.notna(r['Fecha']) else '',
            str(r['Campañas']),
            str(r['Proveedor']),
            str(r['Insumo']),
            str(r['Tipo de insumo']),
            float(r['Cantidad']),
            str(r['Unidad de medida']) if pd.notna(r.get('Unidad de medida')) else '',
            float(r['Precio Unitario']),
            float(r['Total_USD']),
            str(r['Estado Compra']) if pd.notna(r.get('Estado Compra')) else ''
        ])

    # ---------- META ----------
    all_campaigns = sorted(df['Campañas'].unique().tolist())
    all_types = sorted(df['Tipo de insumo'].unique().tolist())
    all_providers = sorted(df['Proveedor'].unique().tolist())

    bundle = {
        "panorama": panorama,
        "insumos": insumos_merged,
        "variacion": variacion,
        "proveedor": proveedor,
        "evolucion": evolucion,
        "meta": {
            "campaigns": all_campaigns,
            "types": all_types,
            "providers": all_providers,
            "total_records": len(compras_slim)
        }
    }

    return bundle, compras_slim


def generar_html(bundle, compras, output_path):
    """Genera el HTML self-contained con datos embebidos."""
    bundle_json = json.dumps(bundle, ensure_ascii=False, separators=(',', ':'))
    compras_json = json.dumps(compras, ensure_ascii=False, separators=(',', ':'))

    # Leer el template HTML
    template_dir = Path(__file__).parent
    template_path = template_dir / 'dashboard_template.html'

    if template_path.exists():
        with open(template_path, 'r', encoding='utf-8') as f:
            html = f.read()
        # Reemplazar placeholders
        html = html.replace("'__BUNDLE_DATA__'", bundle_json)
        html = html.replace("'__COMPRAS_DATA__'", compras_json)
    else:
        log.warning(f"Template no encontrado en {template_path}, usando template inline")
        # Si no hay template, generar uno mínimo
        html = f"""<!DOCTYPE html>
<html><head><title>Dashboard Insumos</title></head>
<body><h1>Error: Template no encontrado</h1>
<p>Colocá el archivo dashboard_template.html en la misma carpeta que este script.</p>
</body></html>"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    log.info(f"  ✓ HTML generado: {output_path} ({size_mb:.1f} MB)")


# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(description='Actualizar Dashboard de Inteligencia de Insumos')
    parser.add_argument('--origen', help='Ruta completa al archivo Cubo_OC_aspx.xlsx')
    parser.add_argument('--onedrive', help='Ruta a la carpeta de OneDrive donde buscar el archivo')
    parser.add_argument('--salida', default='index.html', help='Nombre del archivo HTML de salida (default: index.html)')
    parser.add_argument('--json', action='store_true', help='También guardar los JSONs intermedios en ./data/')
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("  ACTUALIZACIÓN DE DASHBOARD - INTELIGENCIA DE INSUMOS")
    log.info("=" * 60)

    # 1. Encontrar archivo
    ruta = buscar_archivo(args)
    log.info(f"📁 Archivo encontrado: {ruta}")
    log.info(f"   Última modificación: {datetime.fromtimestamp(ruta.stat().st_mtime).strftime('%Y-%m-%d %H:%M')}")

    # 2. Cargar y limpiar
    df = cargar_y_limpiar(ruta)

    # 3. Generar datos
    log.info("Generando datasets...")
    bundle, compras = generar_datos(df)
    log.info(f"  ✓ {len(bundle['insumos'])} insumos, {len(compras)} registros de detalle")

    # 4. Guardar JSONs (opcional)
    if args.json:
        data_dir = Path('./data')
        data_dir.mkdir(exist_ok=True)
        for name, data in [
            ('kpi_panorama.json', bundle['panorama']),
            ('evolución_mensual.json', bundle['evolucion']),
            ('kpi_variación_campañas.json', bundle['variacion']),
            ('kpi_proveedor_precio.json', bundle['proveedor']),
        ]:
            with open(data_dir / name, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        log.info(f"  ✓ JSONs guardados en {data_dir}")

    # 5. Generar HTML
    log.info("Generando HTML self-contained...")
    generar_html(bundle, compras, args.salida)

    log.info("")
    log.info("=" * 60)
    log.info(f"  ✓ LISTO — Abrí {args.salida} en tu navegador")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
