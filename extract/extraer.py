"""Etapa E — extracción de las cuatro fuentes."""
import json
import numpy as np
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

from config import (ARCHIVO_VENTAS, ARCHIVO_FEATURES, ARCHIVO_TIENDAS,
                    ARCHIVO_CALENDARIO, DATOS_PROCESADOS)


def extraer_fuente_primaria():
    """Lee los tres CSV del sistema de ventas."""
    ventas = pd.read_csv(ARCHIVO_VENTAS)
    features = pd.read_csv(ARCHIVO_FEATURES)
    tiendas = pd.read_csv(ARCHIVO_TIENDAS)

    # El formato es día/mes/año: si se deja que pandas lo adivine, mezcla los dos
    for df in (ventas, features):
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

    print(f"  ventas   {ventas.shape[0]:>7,} filas x {ventas.shape[1]} columnas")
    print(f"  features {features.shape[0]:>7,} filas x {features.shape[1]} columnas")
    print(f"  tiendas  {tiendas.shape[0]:>7,} filas x {tiendas.shape[1]} columnas")
    return ventas, features, tiendas


def _dias_al_proximo(fechas, eventos):
    """Días que faltan hasta el próximo evento."""
    eventos = pd.DatetimeIndex(eventos).sort_values()
    posicion = eventos.searchsorted(fechas)
    dias = np.full(len(fechas), np.nan)
    validos = posicion < len(eventos)
    dias[validos] = (eventos[posicion[validos]] - fechas[validos]).days
    return dias


def _dias_desde_el_ultimo(fechas, eventos):
    """Días transcurridos desde el último evento ocurrido en o antes de cada fecha."""
    eventos = pd.DatetimeIndex(eventos).sort_values()
    posicion = eventos.searchsorted(fechas, side="right") - 1
    dias = np.full(len(fechas), np.nan)
    validos = posicion >= 0
    dias[validos] = (fechas[validos] - eventos[posicion[validos]]).days
    return dias


def extraer_fuente_secundaria(semanas):
    """Construye el calendario oficial de festivos y lo publica como JSON anidado.

    El rango arranca en 2009 para que las semanas de 2010 tengan un evento
    anterior del cual medir la distancia.
    """
    from config import DIAS_SEMANA_COMERCIAL

    cal = USFederalHolidayCalendar()
    federales = cal.holidays(start="2009-01-01", end="2013-12-31", return_name=True)

    eventos = {
        "Navidad": pd.to_datetime([f"{a}-12-25" for a in range(2009, 2014)]),
        "AccionGracias": federales[federales == "Thanksgiving Day"].index,
        "LaborDay": federales[federales == "Labor Day"].index,
        "SuperBowl": pd.to_datetime(["2009-02-01", "2010-02-07", "2011-02-06",
                                     "2012-02-05", "2013-02-03"]),
    }

    semanas = pd.DatetimeIndex(sorted(semanas))
    calendario = pd.DataFrame({"Date": semanas})
    for nombre, fechas_ev in eventos.items():
        calendario["Dias_a_" + nombre] = _dias_al_proximo(semanas, fechas_ev)
        calendario["Dias_desde_" + nombre] = _dias_desde_el_ultimo(semanas, fechas_ev)

    # La semana comercial cierra el viernes: contiene el evento si ocurrió en los 6 días previos
    calendario["Contiene_Evento"] = False
    for nombre in eventos:
        calendario["Contiene_Evento"] |= calendario["Dias_desde_" + nombre].between(
            0, DIAS_SEMANA_COMERCIAL)

    # La compra navideña ocurre la semana ANTERIOR a la que contiene el 25 de diciembre
    calendario["Sem_Pre_Navidad"] = calendario["Dias_a_Navidad"].between(
        0, DIAS_SEMANA_COMERCIAL)
    calendario["IsHoliday_Corregido"] = (
        calendario["Contiene_Evento"] | calendario["Sem_Pre_Navidad"]).astype(int)

    _publicar_json(calendario, eventos)
    print(f"  calendario {len(calendario):>5,} semanas · "
          f"{int(calendario.IsHoliday_Corregido.sum())} marcadas como evento")
    return calendario


def _publicar_json(calendario, eventos):
    """Guarda el calendario como JSON anidado, simulando una fuente externa."""
    def num(v):
        return None if pd.isna(v) else int(v)

    registros = []
    for fila in calendario.itertuples():
        registros.append({
            "Date": fila.Date.strftime("%Y-%m-%d"),
            "Contiene_Evento": bool(fila.Contiene_Evento),
            "Sem_Pre_Navidad": bool(fila.Sem_Pre_Navidad),
            "IsHoliday_Corregido": int(fila.IsHoliday_Corregido),
            "Dias_a": {n: num(getattr(fila, "Dias_a_" + n)) for n in eventos},
            "Dias_desde": {n: num(getattr(fila, "Dias_desde_" + n)) for n in eventos},
        })

    DATOS_PROCESADOS.mkdir(parents=True, exist_ok=True)
    with open(ARCHIVO_CALENDARIO, "w", encoding="utf-8") as f:
        json.dump(registros, f, indent=2, ensure_ascii=False)


def leer_calendario_json():
    """Relee el calendario desde el JSON: es la ingesta real de la fuente secundaria."""
    with open(ARCHIVO_CALENDARIO, encoding="utf-8") as f:
        datos = json.load(f)

    cal = pd.json_normalize(datos)
    cal = cal.rename(columns={c: c.replace(".", "_") for c in cal.columns if "." in c})
    cal["Date"] = pd.to_datetime(cal["Date"])
    return cal


def extraer_todo():
    print("[E] Extracción")
    ventas, features, tiendas = extraer_fuente_primaria()
    extraer_fuente_secundaria(features["Date"].unique())
    calendario = leer_calendario_json()
    return ventas, features, tiendas, calendario
