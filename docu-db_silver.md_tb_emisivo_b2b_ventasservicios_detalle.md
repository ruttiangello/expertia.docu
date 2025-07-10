# 📘 Documentación: `md_tb_emisivo_b2b_ventasservicios_detalle`

---

## 🔎 Propósito  
Consolida **transacciones de servicios emisivos** a nivel detallado para:  
- **B2B** (todas las cotizaciones, excepto Corporate)  
- **B2C Franquicias**  

Incluye cotizaciones y ventas de servicios (Journey Assist / TA), comisiones, costos y utilidades, listo para análisis y reporting.

---

## 💰 Campos de Monto  
| Campo                   | Descripción                                                     |
| ----------------------- | --------------------------------------------------------------- |
| **VENTA**               | Importe bruto de venta (con IGV).                               |
| **costo**               | Costo total del servicio (con IGV).                             |
| **costo_neto**          | Costo neto (posiblemente sin impuestos).                       |
| **costo_os**            | Costo asociado a la orden de servicio.                         |
| **pendiente_os**        | Importe pendiente de pago de OS.                               |
| **utilidad**            | Utilidad bruta (`VENTA - costo`).                              |
| **comision_agencia**    | Comisión bruta de agencia.                                     |
| **comision_agencia_neta** | Comisión neta (sin IGV).                                     |
| **comision_ag_neta_2**  | Segunda métrica de comisión neta.                              |
| **Venta_Neta_Comercial**| Venta neta comercial (tras descuentos/comisiones).             |
| **utilidad_neta_sin_igv** | Utilidad neta sin IGV.                                       |
| **venta_sub, venta_neta_sub** | Subtotales de venta por segmento.                       |
| **venta_fc, pendiente_fc, saldo_venta, saldo_costo** | Otros montos de venta y saldos. |
| **rc_ree, op_ree**      | Importes relacionados a reembolsos.                             |

---

## 📂 Ubicación de la Lógica  
Notebook SQL:  
```
06_reglas_de_negocio_listado_de_cotizaciones_sql
```

---

## 📝 Resumen de Secciones

1. **Inicialización & Parámetros**  
   En este bloque se preparan los elementos comunes a todo el proceso:  
   - Se cargan scripts de parámetros globales y utilidades de ingeniería para estandarizar conexiones y funciones.  
   - Se configuran opciones de Spark (activación de caché y particiones automáticas) y se define el esquema Bronze como contexto de trabajo.  
   - Se crean widgets para que el usuario especifique el rango de fechas de procesamiento (`range_start`, `range_end`) y los días a reprocesar, con lógica que asume valores por defecto si no se proporciona entrada.

2. **Lectura y Transformación Previa**  
   Aquí se construye una vista temporal (`_pre`) que recoge todas las cotizaciones crudas:  
   - Se seleccionan los campos esenciales (fechas, montos, estado de cotización, datos de cliente/proveedor).  
   - Se calculan indicadores adicionales (p.ej. flags de venta cero o anulaciones, tipo de producto según aéreo/crucero/auto).  
   - Se aplican reglas para categorizar rutas, destinos y regiones basadas en catálogos auxiliares.  
   - Se enriquecen las cotizaciones con información de calendario (número de semana, año-mes, duración de viaje, días de anticipación).

3. **Carga de Catálogos Auxiliares**  
   Para contextualizar datos, se leen archivos de CSV con:  
   - Grupo de destino (regiones SWV)  
   - Cadena proveedora  
   - Dotación de ejecutivos SVS  
   - Cartera B2B  
   Estos catálogos permiten enriquecer cada registro con atributos como `zona_destino`, `cadena_hotelera`, o asignación de ejecutivo.

4. **Vista Final de Negocio**  
   Se genera la vista definitiva que:  
   - Determina la **unidad de negocio** (`un`, `sub_un`) según tipo de cliente.  
   - Asigna el **negocio** (Inside, Franquicias, Johan, etc.) con prioridad a clientes y ejecutivos específicos.  
   - Define el **equipo** operativo (AGIL Smart, OTROS) según naming patterns y lógica de dotación.  
   - Incluye la información de región y tipo de ruta final (nacional/internacional).  
   - Añade marca de tiempo de ETL.

5. **Carga Incremental**  
   Se invoca el procedimiento de carga incremental, que actualiza únicamente las cotizaciones dentro del rango de fechas definido, ahorrando recursos de procesamiento y evitando recargas completas innecesarias.

6. **Detección y Corrección de Anomalías de Utilidad**  
   - Se identifican cotizaciones con **utilidad neta sin IGV negativa**, agrupando por `id_cotizacion`.  
   - Se ejecuta un `MERGE` sobre la tabla final para marcar automáticamente como “S” aquellos registros que indiquen pérdidas o utilidades negativas, asegurando la calidad de los datos antes de su uso en reporting.


### 1. Inicialización & Parámetros Inicialización & Parámetros  
```python
%run ../initial/global_parameter_py
%run ./00_util_ingenieria_py

target_catalog = default_values['target_catalog']
spark.conf.set("spark.databricks.io.cache.enabled", True)
spark.conf.set("spark.sql.shuffle.partitions","auto")
spark.sql(f"use {target_catalog}.db_bronze")

from datetime import datetime
from dateutil.relativedelta import relativedelta

dbutils.widgets.text('range_start','','01. Fecha inicio:')
dbutils.widgets.text('range_end','','02. Fecha fin:')
dbutils.widgets.text('reprocess_range','','03. Días a reprocesar:')
range_start     = dbutils.widgets.get('range_start')
range_end       = dbutils.widgets.get('range_end')
reprocess_range = int(dbutils.widgets.get('reprocess_range') or 5)

if not (range_start and range_end):
    range_end   = range_end   or str(datetime.now().date())
    range_start = range_start or str((datetime.fromisoformat(range_end) + relativedelta(days=-reprocess_range)).date())

spark.conf.set("parameters.range_start", range_start)
spark.conf.set("parameters.range_end",   range_end)
print(f"Rango: {range_start} → {range_end}")
```

---

### 2. Pre-vista (`VW_md_tb_emisivo_b2b_ventasservicios_detalle_pre`)  
```sql
CREATE OR REPLACE TEMPORARY VIEW VW_md_tb_emisivo_b2b_ventasservicios_detalle_pre AS (
  SELECT
    -- Campos original raw de cotización
    id_cotizacion, fecha_cotizacion, fecha_emision_comprob, 
    VENTA, costo, costo_neto, costo_os, pendiente_os, utilidad,
    comision_agencia, comision_agencia_neta, comision_ag_neta_2,
    Venta_Neta_Comercial, utilidad_neta_sin_igv,
    -- Flags y categorías
    CASE WHEN venta_neta_comercial = 0 THEN 'True' ELSE 'False' END AS venta_cero,
    CASE WHEN comprobante_ext = ' ' THEN 'False' ELSE 'True' END AS anulacion,
    CASE 
      WHEN linea_aerea IN ('SS','RE') THEN 'Tarjetas' 
      WHEN grupo_servicio_nombre LIKE 'TKTS%' THEN 'Boletos'
      ELSE 'Servicios' 
    END AS tipo_producto,
    COALESCE(
      (SELECT first(C1) FROM db_bronze.file_listas_listado_de_cotizaciones_listaciudadesn
       WHERE ciudadhastanombre = ciudad_hasta_nombre),
      'I'
    ) AS tipo_ruta,
    -- Lógica de regiones y categorías de destino...
    FECHAS.week_friday AS semanal,
    YEAR(fecha_cotizacion) AS anio_coti, MONTH(fecha_cotizacion) AS mes_coti,
    YEAR(fecha_entrada) AS anio_entrada,  MONTH(fecha_entrada) AS mes_entrada,
    YEAR(fecha_salida) AS anio_salida,  MONTH(fecha_salida) AS mes_salida,
    DATEDIFF(fecha_entrada,fecha_cotizacion)   AS pre_compra,
    DATEDIFF(fecha_salida,fecha_entrada)       AS duracion,
    -- PAX calculado
    CASE WHEN count_pax = 0 THEN total_chd + total_adl ELSE count_pax END AS pax
  FROM db_bronze.dt_listado_de_cotizaciones
  LEFT JOIN FECHAS ON dt_listado_de_cotizaciones.fecha_emision_comprob = FECHAS.fecha
  WHERE estado_item_adm = 'E'
    AND DATE(fecha_emision_comprob) BETWEEN '${parameters.range_start}'
    AND '${parameters.range_end}'
);
```

---

### 3. Carga de Catálogos Auxiliares  
```python
# Grupo destino, cadena, dotación, cartera B2B
csvs = {
  "GrupoDestino_SVS": "/Volumes/data_bi/db_bronze/files_analistas/GrupoDestino_SVS.csv",
  "Cadena_Proveedor":  "/Volumes/data_bi/db_bronze/files_analistas/Maestro_Cadena_Proveedor.csv",
  "dotacion_svs_202406": "/Volumes/data_bi/db_bronze/files_analistas/Lista_Grupo_202406.csv",
  "file_cartera_b2b_202502":"/Volumes/data_bi/db_bronze/files_analistas/file_cartera_b2b_202502.csv"
}
for view,path in csvs.items():
    spark.read.csv(path, header=True, sep=",", inferSchema=True)          .createOrReplaceTempView(view)
```

---

### 4. Vista Final (`VW_md_tb_emisivo_b2b_ventasservicios_detalle`)  
```sql
CREATE OR REPLACE TEMPORARY VIEW VW_md_tb_emisivo_b2b_ventasservicios_detalle AS (
  SELECT
    a.*,
    -- Unidad de negocio y sub-UN
    CASE
      WHEN tipo_cliente = 'Tercero' THEN 'B2B'
      WHEN tipo_cliente = 'Franquicia' THEN 'Franquicias'
      ELSE 'DM-NM'
    END AS un,
    CASE
      WHEN tipo_cliente = 'Tercero' THEN 'Agil'
      WHEN tipo_cliente = 'Franquicia' THEN 'Franquicias'
      ELSE 'DM-NM'
    END AS sub_un,
    -- Negocio y equipo
    CASE WHEN tipo_cliente = 'Tercero' THEN
      IFNULL(
        CASE WHEN db_parameters.udf_decrypt(nom_cliente) LIKE 'Travel Experiences A-1' THEN 'MANTENIMIENTO 1'
             WHEN db_parameters.udf_decrypt(nom_cliente) LIKE 'FUXION BIOTECH%' THEN 'Johan'
             ELSE c.EJECUTIVO END,
      'INSIDE')
    WHEN tipo_cliente = 'Franquicia' THEN db_parameters.udf_decrypt(nom_cliente)
    ELSE 'Otros' END AS negocio,
    COALESCE(
      CASE WHEN lower(nombre) LIKE '%web%' THEN 'AGIL Smart'
           ELSE COALESCE(e1.Periodo,e2.Grupo) END,
    'OTROS') AS equipo,
    -- Atributos adicionales
    upper(G.Region) AS zona_destino,
    CASE WHEN pais_nombre IN ('Peru','Perú') THEN 'Nacional' ELSE 'Internacional' END AS tipo_ruta2,
    p.cadena AS cadena_hotelera,
    db_parameters.udf_current_timestamp() AS etl_timestamp
  FROM VW_md_tb_emisivo_b2b_ventasservicios_detalle_pre a
  LEFT JOIN td_catalogo_region_emisivo G
    ON a.pais_nombre=G.pais AND a.ciudad_nombre=G.ciudad
  LEFT JOIN Cadena_Proveedor p
    ON a.nombre_proveedor=p.nombreProveedor
  LEFT JOIN dotacion_svs_202406 e1
    ON a.nombre=e1.Ejecutivo AND date_format(a.fecha_emision_comprob,'yyyyMM') < '202407'
  LEFT JOIN file_dotacion_svs e2
    ON a.nombre=e2.Ejecutivo AND date_format(a.fecha_emision_comprob,'yyyyMM') = e2.Periodo
  LEFT JOIN file_cartera_b2b_202502 c
    ON db_parameters.udf_decrypt(a.cliente_documento)=c.RUC
);
```

---

### 5. Carga Incremental 🔄  
```python
results = [create_insert_incremental_table(
  'VW_md_tb_emisivo_b2b_ventasservicios_detalle',
  'fecha_emision_comprob',
  range_start, range_end,
  'db_silver'
)]
display(results)
check_errors(results)
```

---

### 6. Actualización de Anomalías de Utilidad  
```sql
-- Detecta cotizaciones con utilidad negativa
CREATE OR REPLACE TEMPORARY VIEW utilidad_coti AS (
  SELECT id_cotizacion,
         SUM(utilidad_neta_sin_igv) AS total_utilidad_neta_sin_igv
  FROM db_silver.md_tb_emisivo_b2b_ventasservicios_detalle
  WHERE tipo_producto='Servicios' AND a_reembolso=0
  GROUP BY id_cotizacion
  HAVING total_utilidad_neta_sin_igv <= 0
);

-- Merge para marcar estados
MERGE INTO db_silver.md_tb_emisivo_b2b_ventasservicios_detalle AS lista
USING utilidad_coti AS uti_cot
  ON lista.id_cotizacion = uti_cot.id_cotizacion
WHEN MATCHED THEN UPDATE SET
  estado_item_adm = CASE WHEN total_utilidad_neta_sin_igv <= 0 THEN 'S' ELSE estado_item_adm END,
  uti           = CASE WHEN total_utilidad_neta_sin_igv <= 0 THEN 'S' ELSE uti END;
```
