"""Etapa L — carga al esquema de estrella en SQLite y al dataset final."""
import sqlite3
import pandas as pd

from config import DATASET_FINAL, BASE_DW, DATOS_PROCESADOS

ESQUEMA = """
DROP TABLE IF EXISTS hechos_ventas;
DROP TABLE IF EXISTS dim_tienda;
DROP TABLE IF EXISTS dim_fecha;
DROP TABLE IF EXISTS dim_departamento;

CREATE TABLE dim_tienda (
    Store            INTEGER PRIMARY KEY,
    Type             TEXT    NOT NULL,
    Size             INTEGER NOT NULL,
    CPI_medio        REAL,
    Desempleo_medio  REAL
);

CREATE TABLE dim_departamento (
    Dept INTEGER PRIMARY KEY
);

CREATE TABLE dim_fecha (
    Date                 TEXT PRIMARY KEY,
    Anio                 INTEGER,
    Mes                  INTEGER,
    Semana_Anio          INTEGER,
    Trimestre            INTEGER,
    Semana_Del_Mes       INTEGER,
    Es_Diciembre         INTEGER,
    Es_Temporada_Alta    INTEGER,
    IsHoliday            INTEGER,
    IsHoliday_Corregido  INTEGER,
    Sem_Pre_Navidad      INTEGER,
    Dias_a_Navidad       REAL,
    Dias_desde_Navidad   REAL
);

CREATE TABLE hechos_ventas (
    Store              INTEGER NOT NULL,
    Dept               INTEGER NOT NULL,
    Date               TEXT    NOT NULL,
    Weekly_Sales       REAL,
    Ventas_Modelo      REAL,
    Ventas_Winsor      REAL,
    Log_Ventas         REAL,
    Ventas_t_menos_1   REAL,
    Ventas_t_menos_52  REAL,
    Media_Movil_4      REAL,
    Total_MarkDown     REAL,
    MD_Registrado      INTEGER,
    Temperatura_C      REAL,
    CPI_desv           REAL,
    Desempleo_desv     REAL,
    Es_Devolucion      INTEGER,
    Outlier_Z          INTEGER,
    PRIMARY KEY (Store, Dept, Date),
    FOREIGN KEY (Store) REFERENCES dim_tienda(Store),
    FOREIGN KEY (Dept)  REFERENCES dim_departamento(Dept),
    FOREIGN KEY (Date)  REFERENCES dim_fecha(Date)
);
"""


def construir_dimensiones(df):
    """El CPI y el desempleo varían mucho más entre tiendas que en el tiempo:
    el nivel medio pertenece a la dimensión tienda y solo la desviación
    temporal es señal macroeconómica real."""
    nivel = df.groupby("Store")[["CPI", "Unemployment"]].mean()

    dim_tienda = (df[["Store", "Type", "Size"]].drop_duplicates()
                  .merge(nivel.rename(columns={"CPI": "CPI_medio",
                                               "Unemployment": "Desempleo_medio"}),
                         on="Store")
                  .sort_values("Store"))

    dim_dept = pd.DataFrame({"Dept": sorted(df["Dept"].unique())})

    cols_fecha = ["Date", "Anio", "Mes", "Semana_Anio", "Trimestre", "Semana_Del_Mes",
                  "Es_Diciembre", "Es_Temporada_Alta", "IsHoliday",
                  "IsHoliday_Corregido", "Sem_Pre_Navidad",
                  "Dias_a_Navidad", "Dias_desde_Navidad"]
    dim_fecha = df[cols_fecha].drop_duplicates(subset="Date").sort_values("Date").copy()
    for c in ["IsHoliday", "Sem_Pre_Navidad"]:
        dim_fecha[c] = dim_fecha[c].astype(int)

    hechos = df.copy()
    hechos["CPI_desv"] = hechos["CPI"] - hechos["Store"].map(nivel["CPI"])
    hechos["Desempleo_desv"] = (hechos["Unemployment"]
                                - hechos["Store"].map(nivel["Unemployment"]))
    cols_hechos = ["Store", "Dept", "Date", "Weekly_Sales", "Ventas_Modelo",
                   "Ventas_Winsor", "Log_Ventas", "Ventas_t_menos_1",
                   "Ventas_t_menos_52", "Media_Movil_4", "Total_MarkDown",
                   "MD_Registrado", "Temperatura_C", "CPI_desv", "Desempleo_desv",
                   "Es_Devolucion", "Outlier_Z"]
    hechos = hechos[cols_hechos]

    return dim_tienda, dim_dept, dim_fecha, hechos


def cargar_todo(df):
    print("[L] Carga")
    DATOS_PROCESADOS.mkdir(parents=True, exist_ok=True)

    dim_tienda, dim_dept, dim_fecha, hechos = construir_dimensiones(df)

    with sqlite3.connect(BASE_DW) as con:
        con.executescript(ESQUEMA)
        for nombre, tabla in [("dim_tienda", dim_tienda),
                              ("dim_departamento", dim_dept),
                              ("dim_fecha", dim_fecha.assign(
                                  Date=lambda d: d.Date.dt.strftime("%Y-%m-%d"))),
                              ("hechos_ventas", hechos.assign(
                                  Date=lambda d: d.Date.dt.strftime("%Y-%m-%d")))]:
            tabla.to_sql(nombre, con, if_exists="append", index=False)
            print(f"  {nombre:20s} {len(tabla):>7,} filas")

        prueba = pd.read_sql("""
            SELECT t.Type, COUNT(*) AS filas, ROUND(SUM(h.Weekly_Sales)/1e6, 1) AS venta_M
            FROM hechos_ventas h
            JOIN dim_tienda t ON h.Store = t.Store
            JOIN dim_fecha  f ON h.Date  = f.Date
            GROUP BY t.Type ORDER BY t.Type
        """, con)
    print("  consulta de prueba (JOIN de las tres tablas):")
    print(prueba.to_string(index=False).replace("\n", "\n    "))

    df.to_csv(DATASET_FINAL, index=False, compression="gzip")
    print(f"  dataset final: {DATASET_FINAL.name} "
          f"({DATASET_FINAL.stat().st_size/1e6:.1f} MB)")
    return BASE_DW, DATASET_FINAL
