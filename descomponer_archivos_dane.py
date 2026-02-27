import msoffcrypto
import io
import pandas as pd
import numpy as np
import os
from pathlib import Path

# Verificar que openpyxl está instalado
try:
    import openpyxl
    print("✅ openpyxl está instalado")
except ImportError:
    print("❌ openpyxl NO está instalado. Instalando...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
    import openpyxl
    print("✅ openpyxl instalado correctamente")

# ============================================
# CONFIGURACIÓN DE ARCHIVOS DE DEFUNCIONES
# ============================================
archivos_defunciones = [
    {
        'ruta': "data/dane/defunciones-no-fetales2022-cuadro-definitivo-2022.xls",
        'anio': "2022",
        'password': "VelvetSweatshop",
        'encriptado': True,
        'tipo': 'defunciones'
    },
    {
        'ruta': "data/dane/anex-EEVV-DefuncionesNoFetales2023pCuadroDefinitivo-2023.xls",
        'anio': "2023",
        'password': "VelvetSweatshop",
        'encriptado': True,
        'tipo': 'defunciones'
    },
    {
        'ruta': "data/dane/anex-EEVV-DefuncionesNoFetalesCuadroUnificado-2024.xlsx",
        'anio': "2024",
        'password': None,
        'encriptado': False,
        'tipo': 'defunciones'
    },
    {
        'ruta': "data/dane/anex-EEVV-DefuncionesNoFetalesCuadroAnoCorrido-2025pr.xlsx",
        'anio': "2025",
        'password': None,
        'encriptado': False,
        'tipo': 'defunciones'
    }
]

# ============================================
# CONFIGURACIÓN DE ARCHIVOS DE NACIMIENTOS
# ============================================
archivos_nacimientos = [
    {
        'ruta': "data/dane/nacimientos2022p-cuadro-definitivo-ano-corrido-2022pr.xls",
        'anio': "2022",
        'password': "VelvetSweatshop",
        'encriptado': True,
        'tipo': 'nacimientos'
    },
    {
        'ruta': "data/dane/nacimientos2023p-cuadro-ano-corrido-2023.xls",
        'anio': "2023",
        'password': "VelvetSweatshop",
        'encriptado': True,
        'tipo': 'nacimientos'
    },
    {
        'ruta': "data/dane/anex-EEVV-Nacimientos2024pCuadroAnoCorrido-2024.xls",
        'anio': "2024",
        'password': "VelvetSweatshop",
        'encriptado': False,
        'tipo': 'nacimientos'
    },
    {
        'ruta': "data/dane/anex-EEVV-NacimientosCuadroAnoCorrido-2025pr.xlsx",
        'anio': "2025",
        'password': None,
        'encriptado': False,
        'tipo': 'nacimientos'
    }
]

# ============================================
# FUNCIONES COMUNES
# ============================================

def leer_archivo_seguro(ruta, config):
    """Lee cualquier archivo Excel con el engine apropiado"""
    try:
        if not os.path.exists(ruta):
            print(f"  ❌ Archivo no encontrado: {ruta}")
            return None
        
        print(f"  📂 Leyendo: {Path(ruta).name}")
        
        if config['encriptado']:
            # Para archivos .xls encriptados
            try:
                decrypted = io.BytesIO()
                with open(ruta, 'rb') as f:
                    office_file = msoffcrypto.OfficeFile(f)
                    office_file.load_key(password=config['password'])
                    office_file.decrypt(decrypted)
                print(f"  ✅ Desencriptado exitoso")
                return pd.ExcelFile(decrypted, engine='xlrd')
            except Exception as e:
                print(f"  ❌ Error desencriptando: {e}")
                return None
        else:
            # Para archivos .xlsx no encriptados
            try:
                return pd.ExcelFile(ruta, engine='openpyxl')
            except Exception as e:
                print(f"  ❌ Error con openpyxl: {e}")
                try:
                    return pd.ExcelFile(ruta)
                except Exception as e2:
                    print(f"  ❌ También falló: {e2}")
                    return None
    except Exception as e:
        print(f"  ❌ Error general: {e}")
        return None

def limpiar_nombre_columna(nombre):
    if pd.isna(nombre) or nombre == '' or nombre == 'nan':
        return 'columna'
    return str(nombre).strip().replace('\n', ' ').replace('  ', ' ').replace('/', '_').replace('\\', '_')

def detectar_estructura(df_sheet):
    """Detecta dónde empiezan los datos en una hoja"""
    for i in range(min(30, len(df_sheet))):
        fila = df_sheet.iloc[i].fillna('').astype(str)
        texto_fila = ' '.join(fila[:5].tolist()).upper()
        
        patrones_datos = ['TOTAL NACIONAL', 'DEPARTAMENTO', 'GRUPOS DE EDAD', 'TOTAL', 
                         'HOMBRES', 'MUJERES', 'SEXO', 'ÁREA', 'CAUSAS']
        
        if any(patron in texto_fila for patron in patrones_datos):
            valores_numericos = sum([isinstance(x, (int, float)) for x in df_sheet.iloc[i] if not pd.isna(x)])
            if valores_numericos > 0:
                return i, i-1 if i > 0 else i
    
    return None, None

def procesar_hoja(df_sheet, nombre_hoja, anio, tipo):
    """Procesa una hoja y retorna un dataframe limpio"""
    try:
        fila_inicio_datos, fila_encabezados = detectar_estructura(df_sheet)
        
        if fila_inicio_datos is None:
            for i in range(len(df_sheet)):
                fila = df_sheet.iloc[i, :2].astype(str).tolist()
                texto = ' '.join([str(x) for x in fila])
                if "Cuadro" in texto:
                    fila_inicio_datos = i + 3
                    fila_encabezados = i + 2
                    break
        
        if fila_inicio_datos is not None and fila_inicio_datos < len(df_sheet):
            if fila_encabezados is not None and fila_encabezados < len(df_sheet):
                encabezados_raw = df_sheet.iloc[fila_encabezados].fillna('').tolist()
            else:
                encabezados_raw = df_sheet.iloc[fila_inicio_datos - 1].fillna('').tolist()
            
            encabezados = [limpiar_nombre_columna(h) for h in encabezados_raw]
            
            encabezados_unicos = []
            contadores = {}
            for h in encabezados:
                if h in contadores:
                    contadores[h] += 1
                    encabezados_unicos.append(f"{h}_{contadores[h]}")
                else:
                    contadores[h] = 0
                    encabezados_unicos.append(h)
            
            datos = df_sheet.iloc[fila_inicio_datos:].copy()
            datos.columns = encabezados_unicos
            
            datos = datos.dropna(how='all')
            datos = datos.dropna(axis=1, how='all')
            
            if len(datos) > 0:
                mask = ~datos.iloc[:, 0].astype(str).str.contains(
                    'Volver al índice|Cuadro|Fuente|Notas|^$|nan', 
                    na=False, case=False
                )
                datos = datos[mask]
            
            datos = datos.reset_index(drop=True)
            datos['año'] = anio
            datos['tipo'] = tipo
            datos['hoja_origen'] = nombre_hoja
            
            return datos
    except Exception as e:
        print(f"    ⚠️ Error: {e}")
        return None
    
    return None

def procesar_archivos_por_tipo(configuraciones, carpeta_base):
    """Procesa archivos de un tipo específico (defunciones o nacimientos) - VERSIÓN CORREGIDA"""
    
    if not os.path.exists(carpeta_base):
        os.makedirs(carpeta_base)
        print(f"📁 Carpeta base creada: {carpeta_base}")
    
    resumen_total = []
    
    for config in configuraciones:
        ruta = config['ruta']
        anio = config['anio']
        tipo = config['tipo']
        
        print(f"\n{'='*70}")
        print(f"📁 PROCESANDO {tipo.upper()}: {Path(ruta).name} (AÑO {anio})")
        print('='*70)
        
        if not os.path.exists(ruta):
            print(f"❌ Archivo NO existe: {ruta}")
            continue
        
        # Leer el archivo según su tipo
        try:
            if config['encriptado']:
                # Archivo encriptado - desencriptar primero
                decrypted = io.BytesIO()
                with open(ruta, 'rb') as f:
                    office_file = msoffcrypto.OfficeFile(f)
                    office_file.load_key(password=config['password'])
                    office_file.decrypt(decrypted)
                # Leer directamente con pandas sin especificar engine en ExcelFile
                xls = pd.ExcelFile(decrypted)
            else:
                # Archivo no encriptado - leer directamente
                if ruta.endswith('.xlsx'):
                    xls = pd.ExcelFile(ruta, engine='openpyxl')
                else:
                    xls = pd.ExcelFile(ruta, engine='xlrd')
        except Exception as e:
            print(f"❌ Error al leer archivo: {e}")
            # Último intento
            try:
                xls = pd.ExcelFile(ruta)
            except:
                print(f"❌ No se pudo leer el archivo")
                continue
        
        try:
            hojas = xls.sheet_names
            print(f"📑 Hojas: {hojas}")
        except Exception as e:
            print(f"❌ Error leyendo hojas: {e}")
            continue
        
        carpeta_anio = os.path.join(carpeta_base, f"año_{anio}")
        os.makedirs(carpeta_anio, exist_ok=True)
        
        datasets_anio = []
        
        for hoja in hojas:
            print(f"\n  📄 Hoja: {hoja}")
            
            try:
                # Leer la hoja SIN especificar engine (ya está en el ExcelFile)
                df_hoja = pd.read_excel(xls, sheet_name=hoja, header=None)
                
                df_procesado = procesar_hoja_nacimientos(df_hoja, hoja, anio, tipo)
                
                if df_procesado is not None and len(df_procesado) > 0:
                    nombre_archivo = f"{tipo}_{anio}_{hoja.lower().replace(' ', '_').replace('-', '_')}.csv"
                    ruta_archivo = os.path.join(carpeta_anio, nombre_archivo)
                    
                    df_procesado.to_csv(ruta_archivo, index=False, encoding='utf-8-sig')
                    
                    datasets_anio.append({
                        'hoja': hoja,
                        'filas': len(df_procesado),
                        'archivo': nombre_archivo
                    })
                    
                    print(f"    ✅ {len(df_procesado)} filas")
                else:
                    print(f"    ⚠️ Sin datos")
                    
            except Exception as e:
                print(f"    ❌ Error: {e}")
        
        total_filas = sum(d['filas'] for d in datasets_anio)
        print(f"\n  📊 {tipo.upper()} AÑO {anio}: {len(datasets_anio)} datasets, {total_filas:,} filas")
        
        resumen_total.append({
            'tipo': tipo,
            'año': anio,
            'archivo': Path(ruta).name,
            'datasets': len(datasets_anio),
            'filas_totales': total_filas
        })
    
    return resumen_total

# Función específica para procesar hojas de nacimientos (ajustada)
def procesar_hoja_nacimientos(df_sheet, nombre_hoja, anio, tipo):
    """Procesa una hoja de nacimientos"""
    try:
        # Buscar dónde empiezan los datos
        fila_inicio_datos = None
        fila_encabezados = None
        
        for i in range(min(20, len(df_sheet))):
            fila = df_sheet.iloc[i].fillna('').astype(str)
            texto_fila = ' '.join(fila[:5].tolist()).upper()
            
            # Patrones específicos para nacimientos
            if any(patron in texto_fila for patron in ['TOTAL NACIONAL', 'DEPARTAMENTO', 'SEXO', 'ÁREA', 'EDAD']):
                if i > 0:
                    fila_encabezados = i
                    fila_inicio_datos = i + 1
                    break
        
        if fila_inicio_datos is None:
            # Buscar por patrón 'Cuadro'
            for i in range(len(df_sheet)):
                fila = df_sheet.iloc[i, :2].astype(str).tolist()
                texto = ' '.join([str(x) for x in fila])
                if "Cuadro" in texto:
                    fila_inicio_datos = i + 3
                    fila_encabezados = i + 2
                    break
        
        if fila_inicio_datos is not None and fila_inicio_datos < len(df_sheet):
            # Obtener encabezados
            if fila_encabezados is not None and fila_encabezados < len(df_sheet):
                encabezados_raw = df_sheet.iloc[fila_encabezados].fillna('').tolist()
            else:
                encabezados_raw = df_sheet.iloc[fila_inicio_datos - 1].fillna('').tolist()
            
            # Limpiar encabezados
            encabezados = []
            for h in encabezados_raw:
                if pd.isna(h) or h == '' or h == 'nan':
                    encabezados.append('columna')
                else:
                    encabezados.append(str(h).strip().replace('\n', ' '))
            
            # Evitar duplicados
            encabezados_unicos = []
            contadores = {}
            for h in encabezados:
                if h in contadores:
                    contadores[h] += 1
                    encabezados_unicos.append(f"{h}_{contadores[h]}")
                else:
                    contadores[h] = 0
                    encabezados_unicos.append(h)
            
            # Extraer datos
            datos = df_sheet.iloc[fila_inicio_datos:].copy()
            datos.columns = encabezados_unicos
            
            # Limpiar
            datos = datos.dropna(how='all')
            datos = datos.dropna(axis=1, how='all')
            
            # Eliminar filas de metadatos
            if len(datos) > 0:
                mask = ~datos.iloc[:, 0].astype(str).str.contains(
                    'Volver al índice|Cuadro|Fuente|Notas|^$|nan', 
                    na=False, case=False
                )
                datos = datos[mask]
            
            if len(datos) > 0:
                datos = datos.reset_index(drop=True)
                datos['año'] = anio
                datos['tipo'] = tipo
                datos['hoja_origen'] = nombre_hoja
                return datos
            
    except Exception as e:
        print(f"    ⚠️ Error en procesar_hoja_nacimientos: {e}")
    
    return None

# ============================================
# EJECUTAR PROCESAMIENTO
# ============================================

if __name__ == "__main__":
    # Carpeta base para todos los datos
    carpeta_base = "data/datos_completos"
    
    print(f"\n{'='*70}")
    print("PROCESANDO ARCHIVOS DE DEFUNCIONES Y NACIMIENTOS")
    print('='*70)
    
    # Procesar defunciones
    print(f"\n{'#'*70}")
    print("# PROCESANDO DEFUNCIONES")
    print('#'*70)
    
    resumen_defunciones = procesar_archivos_por_tipo(
        archivos_defunciones, 
        os.path.join(carpeta_base, 'defunciones')
    )
    
    # Procesar nacimientos
    print(f"\n{'#'*70}")
    print("# PROCESANDO NACIMIENTOS")
    print('#'*70)
    
    resumen_nacimientos = procesar_archivos_por_tipo(
        archivos_nacimientos, 
        os.path.join(carpeta_base, 'nacimientos')
    )
    
    # ============================================
    # RESUMEN FINAL
    # ============================================
    print(f"\n{'='*70}")
    print("🎯 RESUMEN FINAL - TODOS LOS DATOS")
    print('='*70)
    
    todos_resumenes = resumen_defunciones + resumen_nacimientos
    
    # Resumen por tipo
    print(f"\n📊 POR TIPO DE DATO:")
    for tipo in ['defunciones', 'nacimientos']:
        total_filas_tipo = sum(r['filas_totales'] for r in todos_resumenes if r['tipo'] == tipo)
        total_datasets_tipo = sum(r['datasets'] for r in todos_resumenes if r['tipo'] == tipo)
        print(f"  • {tipo.upper()}: {total_datasets_tipo} datasets, {total_filas_tipo:,} filas")
    
    # Resumen por año
    print(f"\n📊 POR AÑO:")
    for año in ['2022', '2023', '2024', '2025']:
        total_filas_año = sum(r['filas_totales'] for r in todos_resumenes if r['año'] == año)
        print(f"  • {año}: {total_filas_año:,} filas")
    
    # Totales generales
    total_datasets = sum(r['datasets'] for r in todos_resumenes)
    total_filas = sum(r['filas_totales'] for r in todos_resumenes)
    
    print(f"\n✅ GRAN TOTAL:")
    print(f"   • Archivos procesados: {len(archivos_defunciones) + len(archivos_nacimientos)}")
    print(f"   • Total datasets: {total_datasets}")
    print(f"   • Total filas: {total_filas:,}")
    print(f"\n📁 Todos los datos guardados en: {carpeta_base}/")
    print(f"   • Defunciones: {carpeta_base}/defunciones/")
    print(f"   • Nacimientos: {carpeta_base}/nacimientos/")
    
    # Guardar resumen completo
    resumen_df = pd.DataFrame(todos_resumenes)
    resumen_df.to_csv(os.path.join(carpeta_base, 'resumen_completo.csv'), 
                      index=False, encoding='utf-8-sig')
    print(f"\n📄 Resumen guardado en: {carpeta_base}/resumen_completo.csv")