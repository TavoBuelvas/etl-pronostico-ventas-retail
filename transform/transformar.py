"""Etapa T — limpieza, integración y construcción de variables derivadas."""
import numpy as np
import pandas as pd

from config import COLS_MARKDOWN, SEMANAS_MINIMAS


def recortar_al_periodo_con_ventas(features, ventas):
    """Las semanas de Features posteriores a la última venta no sirven y concentran
    el 100 % de los nulos de CPI y desempleo."""
    fin = ventas["Date"].max()
    antes = len(features)
    feat = features[features["Date"] <= fin].copy()
    print(f"  recorte temporal: {antes:,} -> {len(feat):,} filas "
          f"(hasta {fin.date()}) · nulos de CPI: {int(feat['CPI'].isna().sum())}")
    return feat


def tratar_markdown(features):
    """Antes de nov-2011 el dato no se registraba; después, el nulo significa
    'no hubo promoción'. Son dos mecanismos distintos, por eso la bandera."""
    feat = features.copy()
    inicio = feat.dropna(subset=COLS_MARKDOWN, how="all")["Date"].min()
    feat["MD_Registrado"] = (feat["Date"] >= inicio).astype(int)

    negativos = int((feat[COLS_MARKDOWN] < 0).sum().sum())
    for col in COLS_MARKDOWN:
        feat[col] = feat[col].clip(lower=0).fillna(0)
    feat["Total_MarkDown"] = feat[COLS_MARKDOWN].sum(axis=1)

    print(f"  markdown: registro desde {inicio.date()} · "
          f"{negativos} negativos llevados a 0 · nulos restantes "
          f"{int(feat[COLS_MARKDOWN].isna().sum().sum())}")
    return feat


def integrar(ventas, features, tiendas, calendario):
    """Esquema de estrella: ventas es la tabla de hechos; el resto, dimensiones."""
    df = ventas.merge(tiendas, on="Store", how="left", validate="many_to_one")
    df = df.merge(features.drop(columns="IsHoliday"), on=["Store", "Date"],
                  how="left", validate="many_to_one")
    df = df.merge(calendario, on="Date", how="left", validate="many_to_one")

    assert len(df) == len(ventas), "La integración duplicó o perdió filas"
    nulos = df.isna().sum()
    print(f"  integrado: {len(df):,} filas · columnas con nulos: "
          f"{list(nulos[nulos > 0].index) or 'ninguna'}")
    return df


def tratar_ventas_negativas(df):
    """Las negativas son devoluciones o ajustes contables, no errores: se marcan,
    no se eliminan."""
    out = df.copy()
    out["Es_Devolucion"] = (out["Weekly_Sales"] < 0).astype(int)
    out["Ventas_Modelo"] = out["Weekly_Sales"].clip(lower=0)
    print(f"  devoluciones marcadas: {int(out['Es_Devolucion'].sum()):,}")
    return out


def filtrar_series_cortas(df):
    """Una serie con menos de un año no alcanza a mostrar un ciclo estacional."""
    conteo = df.groupby(["Store", "Dept"])["Date"].transform("count")
    antes, series_antes = len(df), df.groupby(["Store", "Dept"]).ngroups
    out = df[conteo >= SEMANAS_MINIMAS].copy()
    print(f"  series cortas excluidas: {antes:,} -> {len(out):,} filas "
          f"({len(out)/antes:.1%} conservado) · "
          f"{series_antes - out.groupby(['Store','Dept']).ngroups} series fuera")
    return out


def tratar_outliers(df):
    """Winsorización solo fuera de semanas de evento: los picos festivos son la
    señal que el proyecto quiere predecir, no ruido."""
    out = df.copy()
    grupo = out.groupby(["Store", "Dept"])["Ventas_Modelo"]
    out["Z_Score"] = ((out["Ventas_Modelo"] - grupo.transform("mean"))
                      / grupo.transform("std")).replace([np.inf, -np.inf], np.nan)
    out["Outlier_Z"] = (out["Z_Score"].abs() > 3).astype(int)

    evento = out["IsHoliday_Corregido"].astype(bool)
    p99 = (out[~evento].groupby(["Store", "Dept"])["Ventas_Modelo"]
           .quantile(0.99).rename("P99_Sin_Evento").reset_index())
    out = out.merge(p99, on=["Store", "Dept"], how="left")

    evento = out["IsHoliday_Corregido"].astype(bool)
    aplicar = (~evento) & out["P99_Sin_Evento"].notna()
    out["Ventas_Winsor"] = np.where(
        aplicar, np.minimum(out["Ventas_Modelo"], out["P99_Sin_Evento"]),
        out["Ventas_Modelo"])

    modificados = int((out["Ventas_Winsor"] != out["Ventas_Modelo"]).sum())
    conservados = int((out["Outlier_Z"].astype(bool) & evento).sum())
    print(f"  outliers: {int(out['Outlier_Z'].sum()):,} detectados · "
          f"{conservados:,} conservados por caer en semana de evento · "
          f"{modificados:,} winsorizados")
    return out


def construir_rezagos(df):
    """Los rezagos se construyen uniendo por fecha exacta, no con shift(): así un
    hueco en la serie produce un nulo honesto y no el valor de otra semana."""
    out = df.copy()
    for k in [1, 2, 3, 4, 52]:
        aux = out[["Store", "Dept", "Date", "Ventas_Winsor"]].copy()
        aux["Date"] = aux["Date"] + pd.Timedelta(weeks=k)
        aux = aux.rename(columns={"Ventas_Winsor": f"Ventas_t_menos_{k}"})
        out = out.merge(aux, on=["Store", "Dept", "Date"], how="left")

    cortos = [f"Ventas_t_menos_{k}" for k in [1, 2, 3, 4]]
    out["Media_Movil_4"] = out[cortos].mean(axis=1)
    out = out.drop(columns=[f"Ventas_t_menos_{k}" for k in [2, 3, 4]])

    print(f"  rezagos: nulos en t-1 {int(out['Ventas_t_menos_1'].isna().sum()):,} · "
          f"en t-52 {int(out['Ventas_t_menos_52'].isna().sum()):,}")
    return out


def construir_derivadas(df):
    """Variables de calendario, normalización por tamaño y codificación."""
    out = df.copy()
    out["Anio"] = out["Date"].dt.year
    out["Mes"] = out["Date"].dt.month
    out["Semana_Anio"] = out["Date"].dt.isocalendar().week.astype(int)
    out["Trimestre"] = out["Date"].dt.quarter
    out["Semana_Del_Mes"] = ((out["Date"].dt.day - 1) // 7) + 1
    out["Es_Diciembre"] = (out["Mes"] == 12).astype(int)
    out["Es_Temporada_Alta"] = out["Mes"].isin([11, 12]).astype(int)

    out["MarkDown_x_Evento"] = out["Total_MarkDown"] * out["IsHoliday_Corregido"]
    out["Ventas_por_1000_Size"] = out["Ventas_Winsor"] / out["Size"] * 1000
    out["Log_Ventas"] = np.log1p(out["Ventas_Winsor"])
    out["Temperatura_C"] = (out["Temperature"] - 32) * 5 / 9

    out = pd.concat(
        [out, pd.get_dummies(out["Type"], prefix="Tipo", dtype=int)], axis=1)

    print(f"  derivadas: {out.shape[1]} columnas en el dataset final")
    return out


def transformar_todo(ventas, features, tiendas, calendario):
    print("[T] Transformación")
    feat = recortar_al_periodo_con_ventas(features, ventas)
    feat = tratar_markdown(feat)
    df = integrar(ventas, feat, tiendas, calendario)
    df = tratar_ventas_negativas(df)
    df = filtrar_series_cortas(df)
    df = tratar_outliers(df)
    df = construir_rezagos(df)
    df = construir_derivadas(df)
    return df
