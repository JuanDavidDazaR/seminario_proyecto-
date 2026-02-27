"""
=======================================================================
 ETL - PASO 2: TRANSFORMACION
 Extrae del cuadro11 los datos necesarios para calcular
 los indicadores PND 2022-2026 en salud.

 INPUT  -> todos_los_cuadro11.csv  (ya procesado)
 OUTPUT -> data/indicadores/       (tablas limpias por indicador)

 Indicadores:
   1. Mortalidad Materna          -> causa "078 Embarazo, parto y puerperio"
   2. Mortalidad Menores de 5     -> todos las causas, grupos edad 0 y 1 (< 5 anos)
   3. Fecundidad 15-19 y 10-14   -> viene del cuadro de nacimientos (separado)

 Estructura cuadro11:
   - columna_2      : nombre de la causa de muerte
   - columna_3      : total de muertes en esa fila
   - Hombres/Mujeres: total por sexo
   - Hombres_N/Mujeres_N (N=1..30): muertes por grupo de edad N
   - La fila con columna_3 maximo por causa/año = Total Nacional
   - Las demas filas = desagregacion por departamento
=======================================================================
"""

import os
import pandas as pd

# --- CONFIGURACION -----------------------------------------------------------
ARCHIVO_CUADRO11 = "todos_los_cuadro11.csv"
CARPETA_SALIDA   = "data/indicadores"

ANIO_INICIO = 2022
ANIO_FIN    = 2024

# Grupos de edad del cuadro11 (Hombres_N / Mujeres_N)
# Segun la metodologia DANE para este cuadro:
# _0 = sin sufijo = todas las edades (total)
# _1 = Menor de 1 ano
# _2 = 1-4 anos
# _3 = 5-9 anos ... etc hasta _30
# Menores de 5 anos = grupos _1 (< 1 ano) + _2 (1-4 anos)
GRUPOS_MENORES_5 = [
    "Hombres_1", "Mujeres_1", "Indeterminado_1",   # menor 1 ano
    "Hombres_2", "Mujeres_2", "Indeterminado_2",   # 1-4 anos
]


# --- HELPERS -----------------------------------------------------------------

def crear_carpeta():
    os.makedirs(CARPETA_SALIDA, exist_ok=True)
    print(f"Carpeta de salida: {CARPETA_SALIDA}")


def cargar_cuadro11():
    if not os.path.exists(ARCHIVO_CUADRO11):
        print(f"[ERROR] No se encontro: {ARCHIVO_CUADRO11}")
        return None
    df = pd.read_csv(ARCHIVO_CUADRO11, low_memory=False)
    print(f"[CARGA] {ARCHIVO_CUADRO11}: {len(df):,} filas x {len(df.columns)} columnas")
    print(f"        Anos disponibles: {sorted(df['año'].dropna().unique().astype(int).tolist())}")
    return df


def filtrar_total_nacional(df, causa_patron, años):
    """
    Para cada año, filtra la causa y se queda solo con el Total Nacional
    (la fila con columna_3 maximo = total nacional).
    Retorna un df con una fila por año.
    """
    df_causa = df[
        df["columna_2"].astype(str).str.contains(causa_patron, case=False, na=False) &
        df["año"].isin(años)
    ].copy()

    # Total Nacional = fila con columna_3 maximo por año
    idx = df_causa.groupby("año")["columna_3"].idxmax()
    df_total = df_causa.loc[idx].reset_index(drop=True)

    return df_total, df_causa


def guardar(df, nombre):
    ruta = os.path.join(CARPETA_SALIDA, f"{nombre}.csv")
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"        Guardado: {ruta}  ({len(df):,} filas)")


# --- INDICADOR 1: MORTALIDAD MATERNA -----------------------------------------

def transform_mortalidad_materna(df):
    print("\n[INDICADOR 1] Mortalidad Materna - 078 Embarazo, parto y puerperio")

    años_principal = list(range(ANIO_INICIO, ANIO_FIN + 1))
    años_historico = [a for a in df["año"].dropna().unique().astype(int) if a < ANIO_INICIO]

    # Total nacional principal
    df_total_p, df_depto_p = filtrar_total_nacional(df, "078", años_principal)
    # Total nacional historico
    df_total_h, df_depto_h = filtrar_total_nacional(df, "078", años_historico)

    # Tabla resumen nacional por año
    cols_base = ["año", "columna_2", "columna_3", "Hombres", "Mujeres", "Indeterminado"]
    df_resumen_p = df_total_p[cols_base].rename(columns={
        "columna_2": "causa",
        "columna_3": "total_muertes_maternas",
    })
    df_resumen_h = df_total_h[cols_base].rename(columns={
        "columna_2": "causa",
        "columna_3": "total_muertes_maternas",
    })

    # Tabla por departamento
    cols_depto = ["año", "columna", "columna_2", "columna_3", "Hombres", "Mujeres"]
    df_depto_p2 = df_depto_p[cols_depto].rename(columns={
        "columna"  : "codigo_depto",
        "columna_2": "causa",
        "columna_3": "muertes_maternas",
    })
    df_depto_h2 = df_depto_h[cols_depto].rename(columns={
        "columna"  : "codigo_depto",
        "columna_2": "causa",
        "columna_3": "muertes_maternas",
    })

    print(f"        Total Nacional 2022-2024:")
    for _, row in df_resumen_p.iterrows():
        print(f"          {int(row['año'])}: {int(row['total_muertes_maternas'])} muertes maternas")

    guardar(df_resumen_p, "ind1_mortalidad_materna_nacional_2022_2024")
    guardar(df_resumen_h, "ind1_mortalidad_materna_nacional_historico")
    guardar(df_depto_p2,  "ind1_mortalidad_materna_departamento_2022_2024")
    guardar(df_depto_h2,  "ind1_mortalidad_materna_departamento_historico")

    return df_resumen_p, df_resumen_h


# --- INDICADOR 2: MORTALIDAD MENORES DE 5 ANOS -------------------------------

def transform_mortalidad_menores_5(df):
    """
    Suma los grupos de edad _1 (< 1 ano) y _2 (1-4 anos) de TODAS las causas.
    Total Nacional = fila con columna_3 maximo por causa/año, luego suma causas.
    """
    print("\n[INDICADOR 2] Mortalidad Menores de 5 anos")

    años_principal = list(range(ANIO_INICIO, ANIO_FIN + 1))
    años_historico = [a for a in df["año"].dropna().unique().astype(int) if a < ANIO_INICIO]

    def calcular_menores5(años, sufijo):
        df_años = df[df["año"].isin(años)].copy()

        # Para cada causa, tomar solo el Total Nacional (max columna_3 por causa/año)
        idx = df_años.groupby(["año", "columna_2"])["columna_3"].idxmax()
        df_nacionales = df_años.loc[idx].copy()

        # Sumar grupos de edad < 5 anos (columnas que existen)
        cols_existentes = [c for c in GRUPOS_MENORES_5 if c in df_nacionales.columns]
        df_nacionales["muertes_menores_1"] = (
            df_nacionales[["Hombres_1", "Mujeres_1", "Indeterminado_1"]]
            .apply(pd.to_numeric, errors="coerce").sum(axis=1)
        )
        df_nacionales["muertes_1_4"] = (
            df_nacionales[["Hombres_2", "Mujeres_2", "Indeterminado_2"]]
            .apply(pd.to_numeric, errors="coerce").sum(axis=1)
        )
        df_nacionales["muertes_menores_5"] = (
            df_nacionales["muertes_menores_1"] + df_nacionales["muertes_1_4"]
        )

        # Agregar por año (suma de todas las causas)
        df_out = (
            df_nacionales.groupby("año")[["muertes_menores_1", "muertes_1_4", "muertes_menores_5"]]
            .sum()
            .reset_index()
            .sort_values("año")
        )
        df_out["indicador"] = "Mortalidad_Menores_5"
        df_out["formula"]   = "Suma todas causas grupos edad <1 ano + 1-4 anos, Total Nacional"
        return df_out

    df_p = calcular_menores5(años_principal, "2022_2024")
    df_h = calcular_menores5(años_historico, "historico")

    print(f"        Total Nacional 2022-2024:")
    for _, row in df_p.iterrows():
        print(f"          {int(row['año'])}: {int(row['muertes_menores_5'])} muertes menores de 5 anos "
              f"(< 1 ano: {int(row['muertes_menores_1'])} | 1-4 anos: {int(row['muertes_1_4'])})")

    guardar(df_p, "ind2_mortalidad_menores5_nacional_2022_2024")
    guardar(df_h, "ind2_mortalidad_menores5_nacional_historico")

    return df_p, df_h


# --- INDICADOR 3 Y 4: NOTA FECUNDIDAD ----------------------------------------

def nota_fecundidad():
    print("\n[INDICADOR 3/4] Fecundidad adolescentes 10-14 y 15-19")
    print("        Estos indicadores requieren el dataset de NACIMIENTOS.")
    print("        El cuadro11 solo contiene defunciones.")
    print("        Pasos a seguir:")
    print("          1. Cargar el dataset de nacimientos ya descargado")
    print("          2. Filtrar por edad_madre entre 10-14 y 15-19")
    print("          3. Agrupar por año")
    print("        Este calculo se hace en transform_nacimientos.py (siguiente script)")


# --- TABLA CONSOLIDADA -------------------------------------------------------

def tabla_consolidada(df_materna_p, df_menores5_p):
    """Une los indicadores de defunciones en una sola tabla resumen."""
    df = df_materna_p[["año", "total_muertes_maternas"]].merge(
        df_menores5_p[["año", "muertes_menores_1", "muertes_1_4", "muertes_menores_5"]],
        on="año", how="outer"
    ).sort_values("año")

    df["nota"] = "Total Nacional - Fuente: DANE Cuadro11 Estadisticas Vitales"
    guardar(df, "consolidado_defunciones_nacional_2022_2024")
    return df


# --- MAIN --------------------------------------------------------------------

def main():
    print("=" * 65)
    print("  ETL PASO 2: TRANSFORMACION INDICADORES PND - DEFUNCIONES")
    print(f"  Periodo principal : {ANIO_INICIO}-{ANIO_FIN}")
    print("  Periodo historico : antes de 2022 (dataset separado)")
    print("  Fuente            : todos_los_cuadro11.csv")
    print("=" * 65)

    crear_carpeta()

    df = cargar_cuadro11()
    if df is None:
        return

    # Transformaciones
    df_materna_p,  df_materna_h  = transform_mortalidad_materna(df)
    df_menores5_p, df_menores5_h = transform_mortalidad_menores_5(df)

    nota_fecundidad()

    # Tabla consolidada
    print("\n[CONSOLIDADO] Uniendo indicadores de defunciones...")
    df_consolidado = tabla_consolidada(df_materna_p, df_menores5_p)

    # Resumen final
    print("\n" + "=" * 65)
    print("  RESUMEN")
    print("-" * 65)
    print(f"  {'Indicador':<45} {'Principal':>10} {'Historico':>10}")
    print("-" * 65)
    print(f"  {'Mortalidad Materna (nacional)':<45} {'OK':>10} {'OK':>10}")
    print(f"  {'Mortalidad Materna (departamento)':<45} {'OK':>10} {'OK':>10}")
    print(f"  {'Mortalidad Menores de 5 anos':<45} {'OK':>10} {'OK':>10}")
    print(f"  {'Fecundidad 10-14 y 15-19':<45} {'pendiente':>10} {'pendiente':>10}")
    print(f"\n  Archivos en: {CARPETA_SALIDA}/")
    print("  Siguiente: transform_nacimientos.py para fecundidad adolescente")
    print("=" * 65)


if __name__ == "__main__":
    main()
