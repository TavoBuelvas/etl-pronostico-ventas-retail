"""Producto de datos — pronóstico de la venta de la semana siguiente."""
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import StandardScaler

from config import FECHA_CORTE_MODELO

REZAGOS = ["Ventas_t_menos_1", "Ventas_t_menos_52", "Media_Movil_4"]

FEATURES = [
    "Size", "Total_MarkDown", "MD_Registrado", "MarkDown_x_Evento",
    "IsHoliday_Corregido", "Sem_Pre_Navidad", "Contiene_Evento",
    "Dias_a_Navidad", "Dias_a_AccionGracias", "Dias_a_LaborDay", "Dias_a_SuperBowl",
    "Anio", "Mes", "Semana_Anio", "Trimestre", "Semana_Del_Mes",
    "Es_Diciembre", "Es_Temporada_Alta",
    "Temperatura_C", "Fuel_Price", "CPI", "Unemployment",
    "Tipo_A", "Tipo_B", "Tipo_C", "Tiene_t52",
] + ["log_" + c for c in REZAGOS]


def preparar(df):
    """Rezagos en logaritmo (igual que el objetivo) y corte temporal."""
    d = df.dropna(subset=["Ventas_t_menos_1", "Media_Movil_4"]).copy()
    d["Tiene_t52"] = d["Ventas_t_menos_52"].notna().astype(int)
    d["Ventas_t_menos_52"] = d["Ventas_t_menos_52"].fillna(0)

    # El objetivo está en logaritmo: los rezagos también, o la relación es curva
    for c in REZAGOS:
        d["log_" + c] = np.log1p(d[c].clip(lower=0))

    d["Sem_Pre_Navidad"] = d["Sem_Pre_Navidad"].astype(int)
    d["Contiene_Evento"] = d["Contiene_Evento"].astype(int)

    train = d[d["Date"] < FECHA_CORTE_MODELO]
    test = d[d["Date"] >= FECHA_CORTE_MODELO]
    print(f"  train {len(train):>7,} filas (hasta {train.Date.max().date()})")
    print(f"  test  {len(test):>7,} filas (desde {test.Date.min().date()})")
    return train, test


def entrenar_y_evaluar(df):
    print("[M] Modelo")
    train, test = preparar(df)

    X_train_crudo, y_train = train[FEATURES], train["Log_Ventas"]
    X_test_crudo, y_test = test[FEATURES], test["Log_Ventas"]

    # El escalador aprende SOLO del pasado: nada posterior al corte lo toca
    escalador = StandardScaler().fit(X_train_crudo)
    X_train = pd.DataFrame(escalador.transform(X_train_crudo), columns=FEATURES)
    X_test = pd.DataFrame(escalador.transform(X_test_crudo), columns=FEATURES)

    real = np.expm1(y_test)
    resultados = {}

    # Referencia sin modelo: la próxima semana vende lo mismo que esta
    resultados["Persistencia (sin modelo)"] = test["Ventas_t_menos_1"].to_numpy()

    lineal = LinearRegression().fit(X_train, y_train)
    resultados["Regresión lineal"] = np.expm1(lineal.predict(X_test))

    try:
        from xgboost import XGBRegressor
        xgb = XGBRegressor(n_estimators=300, max_depth=6, learning_rate=0.1,
                           subsample=0.8, colsample_bytree=0.8,
                           random_state=42, n_jobs=-1)
        xgb.fit(X_train, y_train)
        resultados["XGBoost"] = np.expm1(xgb.predict(X_test))
        importancia = pd.Series(xgb.feature_importances_, index=FEATURES)
    except ImportError:
        print("  (xgboost no está instalado; se omite)")
        importancia = None

    peso = np.where(test["IsHoliday_Corregido"].astype(bool), 5, 1)
    print(f"\n  {'Modelo':28s} {'MAE':>9s} {'RMSE':>9s} {'WMAE':>9s}")
    for nombre, pred in resultados.items():
        mae = mean_absolute_error(real, pred)
        rmse = mean_squared_error(real, pred) ** 0.5
        wmae = np.average(np.abs(real - pred), weights=peso)
        print(f"  {nombre:28s} {mae:>9,.0f} {rmse:>9,.0f} {wmae:>9,.0f}")
    print(f"\n  venta promedio real en el periodo de prueba: {real.mean():,.0f}")

    if importancia is not None:
        print("\n  Variables más influyentes:")
        for nombre, valor in importancia.nlargest(6).items():
            print(f"    {nombre:24s} {valor:.3f}")

    return resultados, real
