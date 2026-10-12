"""Rutas y parámetros compartidos por todas las etapas del pipeline."""
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
DATOS_CRUDOS = RAIZ / "data" / "raw"
DATOS_PROCESADOS = RAIZ / "data" / "processed"
DOCS = RAIZ / "docs"

ARCHIVO_VENTAS = DATOS_CRUDOS / "sales data-set.csv"
ARCHIVO_FEATURES = DATOS_CRUDOS / "Features data set.csv"
ARCHIVO_TIENDAS = DATOS_CRUDOS / "stores data-set.csv"
ARCHIVO_CALENDARIO = DATOS_PROCESADOS / "calendario_festivos.json"

DATASET_FINAL = DATOS_PROCESADOS / "ventas_integrado_limpio.csv.gz"
BASE_DW = DATOS_PROCESADOS / "ventas_dw.sqlite"

# Parámetros de negocio
COLS_MARKDOWN = ["MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5"]
SEMANAS_MINIMAS = 52          # historia mínima de una serie tienda-departamento
FECHA_CORTE_MODELO = "2012-08-01"   # separación temporal train / test
DIAS_SEMANA_COMERCIAL = 6     # la semana cierra el viernes: 6 días hacia atrás
