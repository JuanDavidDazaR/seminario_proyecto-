import requests
import os
import json
import time
import pandas as pd
from datetime import datetime

# Configuración de datasets - Datos Salud (datos.gov.co)
DATASETS = [
    {
        "nombre": "prestadores_servicios_salud_por_departamento",
        "descripcion": "Número de prestadores de servicios de salud por departamento, clase de prestador, año y naturaleza jurídica",
        "url": "https://www.datos.gov.co/resource/kjjp-kasm.json",
        "max_registros": None  # Sin límite → extrae todo
    },
    {
        "nombre": "coberturas_administrativas_vacunacion_por_departamento",
        "descripcion": "Coberturas administrativas de vacunación por departamento",
        "url": "https://www.datos.gov.co/resource/6i25-2hdt.json",
        "max_registros": None  # Sin límite → extrae todo
    },
    {
        "nombre": "indicadores_mortalidad_morbilidad_por_departamento_municipio",
        "descripcion": "Indicadores mortalidad y morbilidad según departamento, municipio y año",
        "url": "https://www.datos.gov.co/resource/4e4i-ua65.json",
        "max_registros": 10000  # Dataset muy grande (+200k), solo últimos 10.000
    }
]

DATA_FOLDER = "data/salud"
LIMIT = 1000          # Registros por página
MAX_REINTENTOS = 5    # Intentos antes de abortar una página
ESPERA_BASE = 3       # Segundos base de espera entre reintentos (se duplica con cada fallo)


def crear_carpeta_data():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        print(f"Carpeta '{DATA_FOLDER}' creada.")
    else:
        print(f"Carpeta '{DATA_FOLDER}' ya existe.")


def obtener_datos_con_reintentos(url, limit, offset):
    """Hace la petición a la API con reintentos y backoff exponencial."""
    params = {"$limit": limit, "$offset": offset}
    espera = ESPERA_BASE

    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            print(f"Timeout/conexión (intento {intento}/{MAX_REINTENTOS}). Reintentando en {espera}s...")

        except requests.exceptions.HTTPError as e:
            codigo = e.response.status_code
            if codigo in (429, 503, 500):
                print(f"Error {codigo} - API saturada (intento {intento}/{MAX_REINTENTOS}). Esperando {espera}s...")
            else:
                raise  # Error no recuperable, se propaga

        time.sleep(espera)
        espera *= 2  # Backoff exponencial: 3s → 6s → 12s → 24s → 48s

    raise Exception(f"Fallaron {MAX_REINTENTOS} intentos en offset {offset}.")


def contar_total_registros(url):
    """Obtiene el total de registros del dataset vía $select=count(*)."""
    try:
        params = {"$select": "count(*)", "$limit": 1}
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return int(data[0]["count"])
    except Exception:
        return None


def extraer_datos(url, nombre, max_registros=None):
    """Extrae datos paginando con control de errores y límite opcional."""
    print(f"\n Extrayendo: {nombre}")

    # Consultar total si aplica
    total = contar_total_registros(url)
    if total:
        print(f"Total registros en API: {total:,}")

    # Calcular offset inicial si hay límite (para traer los más recientes)
    if max_registros and total and total > max_registros:
        offset_inicial = total - max_registros
        print(f"   ⚡ Dataset grande: extrayendo solo los últimos {max_registros:,} registros.")
    else:
        offset_inicial = 0

    todos_los_datos = []
    offset = offset_inicial

    while True:
        # Respetar el límite máximo
        if max_registros:
            registros_restantes = max_registros - len(todos_los_datos)
            if registros_restantes <= 0:
                break
            page_limit = min(LIMIT, registros_restantes)
        else:
            page_limit = LIMIT

        print(f"   → Registros {offset} - {offset + page_limit}...", end=" ")

        try:
            datos = obtener_datos_con_reintentos(url, page_limit, offset)
        except Exception as e:
            print(f"\n   {e}")
            print(f"Abortando dataset. Se guardarán los {len(todos_los_datos):,} registros obtenidos hasta ahora.")
            break

        if not datos:
            print("fin.")
            break

        todos_los_datos.extend(datos)
        print(f"OK ({len(todos_los_datos):,} acumulados)")

        offset += page_limit

        if len(datos) < page_limit:
            break

        # Pausa corta entre páginas para no saturar la API
        time.sleep(0.5)

    print(f"Total registros extraídos: {len(todos_los_datos):,}")
    return todos_los_datos


def guardar_datos(datos, nombre):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{nombre}_{timestamp}"

    archivo_csv = os.path.join(DATA_FOLDER, f"{base}.csv")
    df = pd.DataFrame(datos)
    df.to_csv(archivo_csv, index=False, encoding="utf-8-sig")
    print(f"CSV  → {archivo_csv}")

    archivo_json = os.path.join(DATA_FOLDER, f"{base}.json")
    with open(archivo_json, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
    print(f"JSON → {archivo_json}")


def main():

    print("  Extractor - Datos Salud (datos.gov.co)")

    crear_carpeta_data()

    for dataset in DATASETS:
        print(f"\n{'─' * 60}")
        print(f"{dataset['descripcion']}")
        if dataset["max_registros"]:
            print(f" Límite configurado: {dataset['max_registros']:,} registros")
        try:
            datos = extraer_datos(
                dataset["url"],
                dataset["nombre"],
                dataset.get("max_registros")
            )
            if datos:
                guardar_datos(datos, dataset["nombre"])
            else:
                print("Sin datos encontrados.")
        except Exception as e:
            print(f" Error inesperado: {e}")

    print("¡Completado! Archivos en carpeta 'data/salud/'")



if __name__ == "__main__":
    main()
