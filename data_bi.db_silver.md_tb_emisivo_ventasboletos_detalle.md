# 📘 Documentación | `md_tb_emisivo_ventasboletos_detalle`
## LÓGICA EN EL WORKSPACE: 02_reglas_de_negocio_vista_boletos_excel_emitidos_sql
**Descripción General**  
Esta tabla Silver consolida **todas las transacciones** de venta de boletos aéreos (tickets) y servicios de asistencia (Journey Assist / TA) a nivel global, incluyendo:
- **OTAs** (Online Travel Agencies)  
- **Ventas de seguros y asistencia en viaje**  

Aquí encontrarás métricas clave como:
- Montos de venta neta y bruta  
- Cantidad de tickets y archivos  
- Comisiones, fees y proyecciones de utilidad  
- Detalle de rutas (OW/RT, segmentos, O-D)  

**Propósito**  
Facilitar el análisis y reporte de emisivo en Expertia Travel, unificando datos de múltiples orígenes (Bronze, Silver y catálogos maestros) en un único punto para Power BI y otros consumos de BI.

## Definición de Campos de Monto 💰

A continuación, una breve descripción de cada uno de los campos numéricos de **montos** en `md_tb_emisivo_ventasboletos_detalle`:

| Campo          | Descripción                                                                                               |
| -------------- | --------------------------------------------------------------------------------------------------------- |
| **VentaNeta**  | 🔖 Importe neto de la venta (solo tarifa), sin impuestos ni tasas.                                        |
| **VentaFull**  | 🧾 Importe bruto de la venta (tarifa + impuestos + tasas).                                                 |
| **Fee**        | 💵 Suma de comisiones cobradas sobre la venta (por ejemplo, fees de agencia).                             |
| **Descuento**  | 🔻 Monto total de descuentos aplicados al boleto o servicio.                                               |
| **Com_Std**    | 🏷️ Comisión estándar pagada al vendedor o canal (antes de overs).                                         |
| **Com_Over**   | ➕ Comisión “over” adicional (cuando la comisión real supera la estándar).                                 |
| **Aux**        | 🛠️ Cargos auxiliares (p. ej. tasas administrativas o cargos de emisión).                                   |
| **Proy_BE**    | 📊 Proyección de “business expenses” (factores de costo base proyectados sobre la tarifa).                |
| **Proy_Seg**   | ✈️ Proyección de ingresos o costos por segmento de la ruta (basado en número de tramos).                 |
| **Proy_Uti**   | 💡 Utilidad proyectada: suma de comisiones, fees y proyecciones (BE + SEG) menos descuentos.              |
| **Costo_os**   | ⚙️ Costo de operaciones de servicio (p. ej. cargo interno por procesar la transacción).                  |

> 🔍 **Nota:**  
> - Todos los campos de “Proy_…” son cálculos internos destinados a modelar rentabilidad futura según reglas de negocio.  
> - La lógica completa de cómo se calculan estos montos está en el notebook SQL  
>   `02_reglas_de_negocio_vista_boletos_excel_emitidos_sql`.  


---

## 📑 Tabla de Contenido
1. [Introducción](#introducción)
2. [Parámetros Iniciales 🛠️](#parámetros-iniciales-)
3. [Widgets y Rango de Fechas 📅](#widgets-y-rango-de-fechas-)
4. [Ingestión Bronze: Datos Raw 🥉](#ingestión-bronze-datos-raw-)
5. [Unión de Fuentes: `vista_boletos_merged` 🔗](#unión-de-fuentes-vista_boletos_merged-)
6. [Deducción de Duplicados: `clean_vista_boletos` 🔄](#deducción-de-duplicados-clean_vista_boletos-)
7. [Vista Base Limpia: `VW_vista_boletos_excel` 📋](#vista-base-limpia-vw_vista_boletos_excel-)
8. [Limpieza de Rutas: `df_base` 🧹](#limpieza-de-rutas-df_base-)
9. [Filtrado de Tramos: `df_ruta_0` ✈️](#filtrado-de-tramos-df_ruta_0-)
10. [Numeración de Tramos: `df_ruta_1` 🔢](#numeración-de-tramos-df_ruta_1-)
11. [Flag de Concatenación: `df_ruta_2` 🤝](#flag-de-concatenación-df_ruta_2-)
12. [Construcción de Ruta: `df_ruta_3` 📐](#construcción-de-ruta-df_ruta_3-)
13. [Enriquecimiento y Proyecciones: `df_resultado` 📊](#enriquecimiento-y-proyecciones-df_resultado-)
14. [Códigos Tarifarios: `vw_boletos_codigo_tarifario` 🏷️](#códigos-tarifarios-vw_boletos_codigo_tarifario-)
15. [Bundles: `vw_boletos_bundle` 📦](#bundles-vw_boletos_bundle-)
16. [Correcciones de Destinos: `df_destinos_3` 🗺️](#correcciones-de-destinos-df_destinos_3-)
17. [Merge Final: `nuevomundo2` 🚀](#merge-final-nuevomundo2-)
18. [Vista Silver Final 🥈](#vista-silver-final-)
19. [Carga Incremental 🔄](#carga-incremental-)

---

## 1. Introducción
Este documento describe **paso a paso** la lógica para generar la tabla Silver **`data_bi.db_silver.md_tb_emisivo_ventasboletos_detalle`**, que consolida todas las transacciones de venta de boletos aéreos.

---

## 2. Parámetros Iniciales 🛠️
- **Imports básicos**:
  - `Window`, funciones Spark (`col, when, lead, rank, row_number, regexp_replace, etc.`)
- **Configuración de Spark**:
  - Habilitar cache de I/O (`spark.databricks.io.cache.enabled = True`).
  - `spark.sql.shuffle.partitions = auto`.
  - Uso de la base de datos Bronze: `USE {target_catalog}.db_bronze`.

---

## 3. Widgets y Rango de Fechas 📅
Permiten ejecutar de forma dinámica el notebook:
```python
# Definición en Databricks UI
dbutils.widgets.text('range_start', '', '01. Fecha inicio: ')
dbutils.widgets.text('range_end',   '', '02. Fecha fin: ')
dbutils.widgets.text('reprocess_range', '', '03. Días a reprocesar: ')
# Lógica de defaults (hoy - reprocess_range)
# Se guardan en spark.conf: parameters.range_start / range_end
```

---

## 4. Ingestión Bronze: Datos Raw 🥉
Leemos y creamos vistas temporales de archivos CSV y tablas Bronze:
- **`nv_vista_boletos_excel`**: nueva versión de boletos.
- **`db_bronze.dt_vista_boletos_excel`**: correcciones DT.
- Catálogos auxiliares:
  - `TipoClientes_Corpo` (clientes corporativos).
  - `Cadena_Proveedor` (proveedores).
  - `bundle` (categorías de bundle).
  - `file_listas_analisis_aereos_proybe` → normalizada a `proybe_lit`.

---

## 5. Unión de Fuentes: `vista_boletos_merged` 🔗
Consolidación de múltiples orígenes por **`base`** (empresa/canal):
```sql
SELECT 'PROMOTORA...' AS base, * FROM nuevomundo2 WHERE marca_void=0
UNION ALL
SELECT grupo_empresa AS base, * FROM cp_vista_boletos_excel ...
... + PTA → unionByName(allowMissingColumns=True)
```
Vista resultante: **`vista_boletos_merged`**.

---

## 6. Deducción de Duplicados: `clean_vista_boletos` 🔄
Eliminar registros duplicados por `numero_de_boleto` usando prioridad:
```python
window = Window.partitionBy('numero_de_boleto')  .orderBy(F.when(base==p1,0)....otherwise(5))
df.withColumn('tkt_duplicados', row_number().over(window))  .filter(tkt_duplicados==1)  .createOrReplaceTempView('clean_vista_boletos')
```

---

## 7. Vista Base Limpia: `VW_vista_boletos_excel` 📋
Lectura de **`clean_vista_boletos`** con:
- Desencriptado (UDFs) de cliente/proveedor.
- Cast de fechas (`to_date`).
- Flags: `es_reemision`, `tkt_valido`.
- Cálculos de geografía (O–D), rutas y trozos de itinerario.
- Joins a catálogos: producto, inventario, clasificación de canal, promotores, regiones, equipos.
- Dimensiones temporales: día semana, semana calendario, rangos, mes(año).

---

## 8. Limpieza de Rutas: `df_base` 🧹
Desde la vista cruda:
```python
df_base_0 = spark.sql('SELECT * FROM VW_vista_boletos_excel')
df_base = df_base_0.select(*, regexp_replace(regexp_replace(col('RUTA'),'\s{2,}',''),' ','').alias('Ruta_Limpia2'))
```

---

## 9. Filtrado de Tramos: `df_ruta_0` ✈️
Sólo vuelos aéreos y sin penalidad:
```python
df_ruta_0 = df_base.filter(
  Tipo_de_Producto IN ('Aereo','Aéreo')
  AND Ruta_Limpia2 IS NOT NULL
  AND ES_PENALIDAD == 0
)
.select(Ruta_Limpia2, ID_FILE, PASAJERO, ID_TRANSPORTADOR, NUMERO_DE_BOLETO, ES_CONEXION, FECHA_REPORTE)
```

---

## 10. Numeración de Tramos: `df_ruta_1` 🔢
Asignar posición de tramo dentro de cada boleto:
```python
window = Window.partitionBy('ID_FILE','PASAJERO','ID_TRANSPORTADOR')  .orderBy('NUMERO_DE_BOLETO')
df_ruta_1 = df_ruta_0.select(..., rank().over(window).alias('flag_ruta_rep'))
```

---

## 11. Flag de Concatenación: `df_ruta_2` 🤝
Determina si unir el tramo siguiente:
```python
es_conexion_next = lead('ES_CONEXION',1).over(window)
num_next       = lead('NUMERO_DE_BOLETO',1).over(window)
df_ruta_2 = df_ruta_1.select(*,
  when(es_conexion_next==1 & num_next==NUMERO_DE_BOLETO+1, True).otherwise(False)
    .alias('concatena_siguiente')
)
```

---

## 12. Construcción de Ruta: `df_ruta_3` 📐
Concatena tramos según flag:
```python
Ruta_Limp2_next = lead('Ruta_Limpia2',1).over(window)
df_ruta_3 = df_ruta_2.select(*,
  when(concatena_siguiente, concat(Ruta_Limpia2, Ruta_Limp2_next))
    .otherwise(Ruta_Limpia2)
  .alias('ruta_completa2')
)
```

---

## 13. Enriquecimiento y Proyecciones: `df_resultado` 📊
Combina `df_base` + `df_ruta_3` y calcula:
- `ruta_completa_final` (prioriza ruta_completa2 / Ruta_Limpia2)
- `largo_ruta_completa_final`, `tramos_segmentacion_final`
- `ow_rt_final` (OW vs RT)
- `segmentos_validos`, `proy_seg` (según stock, aerolínea, GDS)
- `proy_ut` (utilidad proyectada) y flag `ut_negativa`

---

## 14. Códigos Tarifarios: `vw_boletos_codigo_tarifario` 🏷️
Explode `farebasis`, extrae segmentos según aerolínea con `substring`:
```sql
SELECT DISTINCT numero_de_boleto, ..., farebasis_exp,
  CASE WHEN aerolínea IN (...) THEN SUBSTRING(...) END AS codigo_tarifario
FROM VW_md_tb_emisivo_ventasboletos_detalle_pre
LATERAL VIEW explode(split(farebasis,',')) t AS farebasis_exp
```

---

## 15. Bundles: `vw_boletos_bundle` 📦
Une con tabla `bundle`, asigna prioridad y filtra `rn=1`:
```sql
WITH tabla1 AS (... prioridad ...),
 tabla2 AS (... row_number() OVER PARTITION ...),
 tabla3 AS (SELECT * FROM tabla2 WHERE rn=1)
SELECT * FROM tabla3
```

---
