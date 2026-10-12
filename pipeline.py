"""Orquestador del pipeline ETL completo: extract -> transform -> load -> modelo.

Uso:
    python pipeline.py            pipeline completo + modelo
    python pipeline.py --sin-modelo   solo ETL
"""
import sys
import time

from extract.extraer import extraer_todo
from transform.transformar import transformar_todo
from load.cargar import cargar_todo


def main(con_modelo=True):
    inicio = time.time()
    print("=" * 62)
    print("PIPELINE ETL — Pronóstico de ventas semanales")
    print("=" * 62)

    ventas, features, tiendas, calendario = extraer_todo()
    df = transformar_todo(ventas, features, tiendas, calendario)
    base, dataset = cargar_todo(df)

    if con_modelo:
        from modelo.entrenar import entrenar_y_evaluar
        entrenar_y_evaluar(df)

    print("=" * 62)
    print(f"Pipeline completo en {time.time() - inicio:.1f} s")
    print(f"  base de datos : {base}")
    print(f"  dataset final : {dataset}")
    print("=" * 62)


if __name__ == "__main__":
    main(con_modelo="--sin-modelo" not in sys.argv)
