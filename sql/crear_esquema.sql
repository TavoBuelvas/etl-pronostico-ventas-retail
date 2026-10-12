-- ============================================================================
-- Recreación del esquema de estrella — Bodega de ventas semanales
--
-- Proyecto : Pronóstico de ventas semanales por tienda y departamento
-- Curso    : ETL — Maestría en IA y Ciencia de Datos, UAO, 2026-2
-- Motor    : SQLite 3
--
-- Este script crea la estructura vacía de la bodega. Los datos se cargan
-- ejecutando el pipeline:   python pipeline.py
--
-- Uso:
--   sqlite3 data/processed/ventas_dw.sqlite < sql/crear_esquema.sql
--
-- El script es idempotente: cada ejecución borra las tablas y las vuelve a
-- crear, de modo que correrlo diez veces deja el mismo resultado que correrlo
-- una. Es la misma propiedad que tiene el pipeline y la razón por la que un
-- lote que se cae a la mitad se puede relanzar sin dejar registros duplicados.
-- ============================================================================

-- SQLite no valida las llaves foráneas a menos que se le pida explícitamente,
-- y la configuración se pierde al cerrar la conexión. Hay que activarla en
-- cada sesión, antes de insertar.
PRAGMA foreign_keys = ON;

-- El orden importa: primero se borra la tabla que apunta a las otras.
DROP TABLE IF EXISTS hechos_ventas;
DROP TABLE IF EXISTS dim_tienda;
DROP TABLE IF EXISTS dim_fecha;
DROP TABLE IF EXISTS dim_departamento;


-- ----------------------------------------------------------------------------
-- DIMENSIÓN TIENDA
--
-- CPI_medio y Desempleo_medio viven aquí, no en los hechos, porque estos dos
-- indicadores varían mucho más entre tiendas que a lo largo del tiempo: el
-- nivel medio identifica la región de la tienda. Solo la desviación respecto
-- a ese nivel es señal macroeconómica real, y esa sí va en los hechos.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_tienda (
    Store            INTEGER PRIMARY KEY,
    Type             TEXT    NOT NULL,      -- A, B o C
    Size             INTEGER NOT NULL,      -- superficie en pies cuadrados
    CPI_medio        REAL,                  -- nivel medio del IPC en el periodo
    Desempleo_medio  REAL                   -- nivel medio de desempleo
);


-- ----------------------------------------------------------------------------
-- DIMENSIÓN DEPARTAMENTO
--
-- Dimensión degenerada: la fuente no trae nombre ni categoría, solo el número.
-- Se conserva como tabla para que las tres llaves foráneas de los hechos
-- apunten a dimensiones reales, y para dejar listo el punto de extensión el
-- día que exista el catálogo de departamentos.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_departamento (
    Dept  INTEGER PRIMARY KEY
);


-- ----------------------------------------------------------------------------
-- DIMENSIÓN FECHA
--
-- El grano es la semana comercial, que cierra el viernes. IsHoliday se
-- conserva junto a IsHoliday_Corregido a propósito: la original venía mal
-- (marcaba la semana posterior al pico de compra) y tener las dos permite
-- auditar la corrección en lugar de solo afirmarla.
-- ----------------------------------------------------------------------------
CREATE TABLE dim_fecha (
    Date                 TEXT PRIMARY KEY,  -- viernes de cierre, 'YYYY-MM-DD'
    Anio                 INTEGER,
    Mes                  INTEGER,
    Semana_Anio          INTEGER,           -- número de semana ISO
    Trimestre            INTEGER,
    Semana_Del_Mes       INTEGER,           -- 1 a 5
    Es_Diciembre         INTEGER,           -- 0 / 1
    Es_Temporada_Alta    INTEGER,           -- 0 / 1 — noviembre o diciembre
    IsHoliday            INTEGER,           -- bandera ORIGINAL, para auditoría
    IsHoliday_Corregido  INTEGER,           -- bandera reconstruida — la del modelo
    Sem_Pre_Navidad      INTEGER,           -- 0 / 1
    Dias_a_Navidad       REAL,
    Dias_desde_Navidad   REAL
);


-- ----------------------------------------------------------------------------
-- TABLA DE HECHOS
--
-- Grano: una tienda, un departamento, una semana.
-- La llave primaria compuesta impide por diseño que se cuele un duplicado,
-- incluso si el pipeline se ejecutara dos veces sobre la misma base.
--
-- Weekly_Sales conserva los valores negativos (devoluciones y ajustes
-- contables) para la trazabilidad; Ventas_Modelo tiene piso en 0 y
-- Ventas_Winsor es la variable objetivo del modelo.
-- ----------------------------------------------------------------------------
CREATE TABLE hechos_ventas (
    Store              INTEGER NOT NULL,
    Dept               INTEGER NOT NULL,
    Date               TEXT    NOT NULL,

    -- medidas
    Weekly_Sales       REAL,                -- venta original, puede ser negativa
    Ventas_Modelo      REAL,                -- venta con piso en 0
    Ventas_Winsor      REAL,                -- objetivo: winsorizada fuera de evento
    Log_Ventas         REAL,                -- log(1 + Ventas_Winsor)

    -- rezagos, construidos por unión de fecha exacta (no con shift)
    Ventas_t_menos_1   REAL,
    Ventas_t_menos_52  REAL,
    Media_Movil_4      REAL,

    -- contexto de la semana
    Total_MarkDown     REAL,
    MD_Registrado      INTEGER,             -- 1 si la promoción sí se medía
    Temperatura_C      REAL,
    CPI_desv           REAL,                -- desviación sobre el nivel de la tienda
    Desempleo_desv     REAL,

    -- banderas de calidad
    Es_Devolucion      INTEGER,
    Outlier_Z          INTEGER,

    PRIMARY KEY (Store, Dept, Date),
    FOREIGN KEY (Store) REFERENCES dim_tienda(Store),
    FOREIGN KEY (Dept)  REFERENCES dim_departamento(Dept),
    FOREIGN KEY (Date)  REFERENCES dim_fecha(Date)
);


-- ----------------------------------------------------------------------------
-- ÍNDICES OPCIONALES
--
-- La llave primaria ya indexa (Store, Dept, Date) en ese orden, así que las
-- consultas que filtran por tienda ya están cubiertas. Falta el camino
-- contrario: "qué pasó en esta semana en toda la cadena" recorre la tabla
-- completa sin un índice sobre Date.
--
-- Están comentados a propósito, para que este script quede idéntico al DDL
-- que ejecuta load/cargar.py y no haya dos definiciones distintas del mismo
-- esquema. Descoméntalos si vas a consultar la bodega a mano con frecuencia.
-- ----------------------------------------------------------------------------
-- CREATE INDEX idx_hechos_fecha ON hechos_ventas(Date);
-- CREATE INDEX idx_hechos_dept  ON hechos_ventas(Dept);


-- ============================================================================
-- CONSULTAS DE VERIFICACIÓN
-- Ejecutar después de cargar con el pipeline.
-- ============================================================================

-- 1. Conteo por tabla. Esperado: 45 / 76 / 143 / 416.615
-- SELECT 'dim_tienda' AS tabla, COUNT(*) AS filas FROM dim_tienda
-- UNION ALL SELECT 'dim_departamento', COUNT(*) FROM dim_departamento
-- UNION ALL SELECT 'dim_fecha',        COUNT(*) FROM dim_fecha
-- UNION ALL SELECT 'hechos_ventas',    COUNT(*) FROM hechos_ventas;

-- 2. Integridad referencial: las tres consultas deben devolver 0 filas.
-- SELECT COUNT(*) FROM hechos_ventas h
--   LEFT JOIN dim_tienda t ON h.Store = t.Store WHERE t.Store IS NULL;
-- SELECT COUNT(*) FROM hechos_ventas h
--   LEFT JOIN dim_departamento d ON h.Dept = d.Dept WHERE d.Dept IS NULL;
-- SELECT COUNT(*) FROM hechos_ventas h
--   LEFT JOIN dim_fecha f ON h.Date = f.Date WHERE f.Date IS NULL;

-- 3. Venta total por tipo de tienda. JOIN de tres tablas.
-- SELECT t.Type,
--        COUNT(*)                          AS filas,
--        ROUND(SUM(h.Weekly_Sales)/1e6, 1) AS venta_millones
-- FROM hechos_ventas h
-- JOIN dim_tienda t ON h.Store = t.Store
-- JOIN dim_fecha  f ON h.Date  = f.Date
-- GROUP BY t.Type ORDER BY t.Type;

-- 4. Evidencia de la corrección de la bandera de festivos: las semanas donde
--    la original y la corregida no coinciden.
-- SELECT Date, IsHoliday, IsHoliday_Corregido, Sem_Pre_Navidad
-- FROM dim_fecha
-- WHERE IsHoliday <> IsHoliday_Corregido
-- ORDER BY Date;

-- 5. Las diez semanas de mayor venta de la cadena, con su marca de evento.
--    Sirve para mostrar que las cuatro primeras quedaron marcadas.
-- SELECT f.Date,
--        ROUND(SUM(h.Weekly_Sales)/1e6, 1) AS venta_millones,
--        f.IsHoliday, f.IsHoliday_Corregido
-- FROM hechos_ventas h
-- JOIN dim_fecha f ON h.Date = f.Date
-- GROUP BY f.Date
-- ORDER BY SUM(h.Weekly_Sales) DESC
-- LIMIT 10;
