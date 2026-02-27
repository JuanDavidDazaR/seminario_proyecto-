import pandas as pd
import os
import glob

# ============================================
# CONFIGURACIÓN DE RUTAS
# ============================================
carpeta_base = 'data/datos_completos'
carpeta_destino = 'data/medio_proceso'

# Crear carpeta de destino si no existe
if not os.path.exists(carpeta_destino):
    os.makedirs(carpeta_destino)
    print(f"📁 Carpeta creada: {carpeta_destino}")

# ============================================
# FUNCIÓN PARA UNIR CUADROS DE DEFUNCIONES
# ============================================
def unir_cuadro_defunciones(nombre_cuadro):
    """
    Une un cuadro específico de defunciones de todos los años
    Ej: 'cuadro1', 'cuadro5', 'cuadro11', etc.
    """
    print(f"\n{'='*70}")
    print(f"UNIENDO DEFUNCIONES - {nombre_cuadro.upper()} - TODOS LOS AÑOS")
    print('='*70)
    
    dataframes = []
    total_filas = 0
    
    for año in ['2022', '2023', '2024', '2025']:
        carpeta_año = os.path.join(carpeta_base, 'defunciones', f'año_{año}')
        patron = os.path.join(carpeta_año, f'defunciones_{año}_{nombre_cuadro.lower()}*.csv')
        archivos = glob.glob(patron)
        
        if archivos:
            archivo = archivos[0]
            df = pd.read_csv(archivo)
            
            # Asegurar columnas identificadoras
            df['año'] = año
            df['tipo'] = 'defunciones'
            df['cuadro'] = nombre_cuadro
            
            dataframes.append(df)
            print(f"  ✅ Año {año}: {len(df):,} filas")
            total_filas += len(df)
        else:
            print(f"  ❌ Año {año}: No encontrado")
    
    if dataframes:
        df_unido = pd.concat(dataframes, ignore_index=True)
        
        print(f"\n📊 TOTAL DEFUNCIONES {nombre_cuadro.upper()}:")
        print(f"   • Filas totales: {len(df_unido):,}")
        print(f"   • Columnas: {len(df_unido.columns)}")
        
        # Guardar
        nombre_archivo = f"defunciones_todos_los_{nombre_cuadro}.csv"
        ruta_completa = os.path.join(carpeta_destino, nombre_archivo)
        df_unido.to_csv(ruta_completa, index=False, encoding='utf-8-sig')
        print(f"\n💾 Guardado en: {ruta_completa}")
        
        return df_unido
    else:
        print("❌ No se encontraron datos")
        return None

# ============================================
# FUNCIÓN PARA UNIR CUADROS DE NACIMIENTOS
# ============================================
def unir_cuadro_nacimientos(nombre_cuadro):
    """
    Une un cuadro específico de nacimientos de todos los años
    Ej: 'cuadro1', 'cuadro3', 'cuadro5', 'cuadro6a', 'cuadro11', 'cuadro13'
    """
    print(f"\n{'='*70}")
    print(f"UNIENDO NACIMIENTOS - {nombre_cuadro.upper()} - TODOS LOS AÑOS")
    print('='*70)
    
    dataframes = []
    total_filas = 0
    
    for año in ['2022', '2023', '2024', '2025']:
        carpeta_año = os.path.join(carpeta_base, 'nacimientos', f'año_{año}')
        patron = os.path.join(carpeta_año, f'nacimientos_{año}_{nombre_cuadro.lower()}*.csv')
        archivos = glob.glob(patron)
        
        if archivos:
            archivo = archivos[0]
            df = pd.read_csv(archivo)
            
            # Asegurar columnas identificadoras
            df['año'] = año
            df['tipo'] = 'nacimientos'
            df['cuadro'] = nombre_cuadro
            
            dataframes.append(df)
            print(f"  ✅ Año {año}: {len(df):,} filas")
            total_filas += len(df)
        else:
            print(f"  ❌ Año {año}: No encontrado")
    
    if dataframes:
        df_unido = pd.concat(dataframes, ignore_index=True)
        
        print(f"\n📊 TOTAL NACIMIENTOS {nombre_cuadro.upper()}:")
        print(f"   • Filas totales: {len(df_unido):,}")
        print(f"   • Columnas: {len(df_unido.columns)}")
        
        # Guardar
        nombre_archivo = f"nacimientos_todos_los_{nombre_cuadro}.csv"
        ruta_completa = os.path.join(carpeta_destino, nombre_archivo)
        df_unido.to_csv(ruta_completa, index=False, encoding='utf-8-sig')
        print(f"\n💾 Guardado en: {ruta_completa}")
        
        return df_unido
    else:
        print("❌ No se encontraron datos")
        return None

# ============================================
# FUNCIÓN PARA UNIR MEGA DATASET DE DEFUNCIONES
# ============================================
def unir_mega_defunciones():
    """
    Une TODOS los cuadros de defunciones en un mega dataset
    """
    print(f"\n{'='*70}")
    print("MEGA DATASET - DEFUNCIONES (TODOS LOS CUADROS)")
    print('='*70)
    
    todos_dfs = []
    cuadros_defunciones = ['cuadro1', 'cuadro2', 'cuadro3', 'cuadro4', 'cuadro5', 
                          'cuadro6', 'cuadro7', 'cuadro8', 'cuadro9', 'cuadro10',
                          'cuadro11', 'cuadro12', 'cuadro13', 'cuadro14', 'cuadro15', 'cuadro16']
    
    for cuadro in cuadros_defunciones:
        for año in ['2022', '2023', '2024', '2025']:
            carpeta_año = os.path.join(carpeta_base, 'defunciones', f'año_{año}')
            patron = os.path.join(carpeta_año, f'defunciones_{año}_{cuadro}*.csv')
            archivos = glob.glob(patron)
            
            if archivos:
                df = pd.read_csv(archivos[0])
                df['año'] = año
                df['tipo'] = 'defunciones'
                df['cuadro'] = cuadro
                todos_dfs.append(df)
                print(f"  ✅ {año} - {cuadro}: {len(df):,} filas")
    
    if todos_dfs:
        df_mega = pd.concat(todos_dfs, ignore_index=True, sort=False)
        
        print(f"\n📊 MEGA DEFUNCIONES:")
        print(f"   • Filas totales: {len(df_mega):,}")
        print(f"   • Columnas: {len(df_mega.columns)}")
        print(f"   • Cuadros: {df_mega['cuadro'].nunique()}")
        
        # Guardar
        ruta = os.path.join(carpeta_destino, "MEGA_DATASET_DEFUNCIONES.csv")
        df_mega.to_csv(ruta, index=False, encoding='utf-8-sig')
        print(f"\n💾 Guardado en: {ruta}")
        
        return df_mega
    
    return None

# ============================================
# FUNCIÓN PARA UNIR MEGA DATASET DE NACIMIENTOS
# ============================================
def unir_mega_nacimientos():
    """
    Une TODOS los cuadros de nacimientos en un mega dataset
    """
    print(f"\n{'='*70}")
    print("MEGA DATASET - NACIMIENTOS (TODOS LOS CUADROS)")
    print('='*70)
    
    todos_dfs = []
    cuadros_nacimientos = ['cuadro1', 'cuadro3', 'cuadro5', 'cuadro6a', 'cuadro11', 'cuadro13']
    
    for cuadro in cuadros_nacimientos:
        for año in ['2022', '2023', '2024', '2025']:
            carpeta_año = os.path.join(carpeta_base, 'nacimientos', f'año_{año}')
            patron = os.path.join(carpeta_año, f'nacimientos_{año}_{cuadro}*.csv')
            archivos = glob.glob(patron)
            
            if archivos:
                df = pd.read_csv(archivos[0])
                df['año'] = año
                df['tipo'] = 'nacimientos'
                df['cuadro'] = cuadro
                todos_dfs.append(df)
                print(f"  ✅ {año} - {cuadro}: {len(df):,} filas")
    
    if todos_dfs:
        df_mega = pd.concat(todos_dfs, ignore_index=True, sort=False)
        
        print(f"\n📊 MEGA NACIMIENTOS:")
        print(f"   • Filas totales: {len(df_mega):,}")
        print(f"   • Columnas: {len(df_mega.columns)}")
        print(f"   • Cuadros: {df_mega['cuadro'].nunique()}")
        
        # Guardar
        ruta = os.path.join(carpeta_destino, "MEGA_DATASET_NACIMIENTOS.csv")
        df_mega.to_csv(ruta, index=False, encoding='utf-8-sig')
        print(f"\n💾 Guardado en: {ruta}")
        
        return df_mega
    
    return None

# ============================================
# FUNCIÓN PARA VERIFICAR ARCHIVOS GUARDADOS
# ============================================
def verificar_archivos_guardados():
    print(f"\n{'='*70}")
    print("VERIFICANDO ARCHIVOS EN /data/medio_proceso/")
    print('='*70)
    
    if not os.path.exists(carpeta_destino):
        print(f"❌ La carpeta {carpeta_destino} no existe")
        return
    
    archivos = glob.glob(os.path.join(carpeta_destino, '*.csv'))
    
    if archivos:
        print(f"\n📁 Archivos encontrados:")
        total_filas = 0
        defunciones_filas = 0
        nacimientos_filas = 0
        
        for archivo in sorted(archivos):
            df = pd.read_csv(archivo)
            nombre = os.path.basename(archivo)
            print(f"  • {nombre}: {len(df):,} filas, {len(df.columns)} columnas")
            total_filas += len(df)
            
            if 'defunciones' in nombre:
                defunciones_filas += len(df)
            elif 'nacimientos' in nombre:
                nacimientos_filas += len(df)
        
        print(f"\n✅ TOTAL GENERAL: {len(archivos)} archivos, {total_filas:,} filas")
        print(f"   • Defunciones: {defunciones_filas:,} filas")
        print(f"   • Nacimientos: {nacimientos_filas:,} filas")
    else:
        print(f"❌ No se encontraron archivos CSV")

# ============================================
# EJECUTAR TODO
# ============================================
print(f"\n{'='*70}")
print("🚀 INICIANDO PROCESO DE UNIÓN DE DATOS")
print('='*70)

# 1. UNIR CUADROS ESPECÍFICOS DE DEFUNCIONES
print("\n📦 UNIENDO CUADROS DE DEFUNCIONES...")
df_def_cuadro1 = unir_cuadro_defunciones('cuadro1')
df_def_cuadro3 = unir_cuadro_defunciones('cuadro3')
df_def_cuadro5 = unir_cuadro_defunciones('cuadro5')
df_def_cuadro11 = unir_cuadro_defunciones('cuadro11')

# 2. UNIR CUADROS ESPECÍFICOS DE NACIMIENTOS
print("\n📦 UNIENDO CUADROS DE NACIMIENTOS...")
df_nac_cuadro1 = unir_cuadro_nacimientos('cuadro1')
df_nac_cuadro3 = unir_cuadro_nacimientos('cuadro3')
df_nac_cuadro5 = unir_cuadro_nacimientos('cuadro5')
df_nac_cuadro6a = unir_cuadro_nacimientos('cuadro6a')
df_nac_cuadro11 = unir_cuadro_nacimientos('cuadro11')
df_nac_cuadro13 = unir_cuadro_nacimientos('cuadro13')

# 3. CREAR MEGA DATASETS
print("\n📦 CREANDO MEGA DATASETS...")
df_mega_def = unir_mega_defunciones()
df_mega_nac = unir_mega_nacimientos()

# 4. VERIFICAR RESULTADOS
verificar_archivos_guardados()

print(f"\n{'='*70}")
print("✅ PROCESO COMPLETADO EXITOSAMENTE")
print(f"📁 Todos los archivos guardados en: {carpeta_destino}")
print('='*70)

# Mostrar resumen rápido
print(f"\n📊 RESUMEN RÁPIDO:")
print(f"   • Defunciones - Cuadros unidos: cuadro1, cuadro3, cuadro5, cuadro11")
print(f"   • Nacimientos - Cuadros unidos: cuadro1, cuadro3, cuadro5, cuadro6a, cuadro11, cuadro13")
print(f"   • Mega datasets: DEFUNCIONES y NACIMIENTOS")