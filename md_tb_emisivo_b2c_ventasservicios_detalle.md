# 📘 Documentación: `md_tb_emisivo_b2c_ventasservicios_detalle`

---

## 🔎 Propósito  
Consolida **transacciones de servicios emisivos** a nivel detalle para:  
- **B2C** (Retail + Web)  
- **B2B Corporate**  

Incluye ventas de servicios (Journey Assist / TA), fees y comisiones asociadas, listo para análisis y reporting.

---

## 💰 Campos de Monto  
| Campo                   | Descripción                                                              |
| ----------------------- | ------------------------------------------------------------------------ |
| **venta_neta**          | Importe neto de servicios vendidos (sin impuestos ni recargos).         |
| **venta_total**         | Monto bruto (gravado + igv + inafecto + no_gravado + srv_recargo_consumo). |
| **venta_neta_usd**      | Venta neta convertida a USD.                                            |
| **venta_total_usd**     | Venta total convertida a USD.                                           |
| **costo_total_usd**     | Costo de servicio con IGV en USD.                                       |
| **utilidad_total**      | `venta_total_usd – costo_total_usd`.                                     |
| **utilidad_neta_usd**   | Utilidad después de impuestos (`utilidad_total ÷ 1.18`).                |
| **uti_fee_comisiones**  | Utilidad atribuible a fees y comisiones.                                |
| **uti_servicios_usd**   | Utilidad específica de servicios (solo si `id_orden_de_servicio > 1`).   |
| **uti_neta_total_fee_svs** | Suma de `uti_fee_comisiones` + `uti_servicios_usd`.                |

---

## 📂 Ubicación de la Lógica  
Todas las transformaciones y reglas están en el notebook SQL:  
```
03_reglas_de_negocio_up_sel_analisis_de_ventas_srv
```

---

## 📝 Resumen de Secciones con Código

### 1. Inicialización & Parámetros  
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
range_start       = dbutils.widgets.get('range_start')
range_end         = dbutils.widgets.get('range_end')
reprocess_range   = int(dbutils.widgets.get('reprocess_range') or 65)

if not (range_start and range_end):
    range_end   = range_end   or str(datetime.now().date())
    range_start = range_start or str((datetime.fromisoformat(range_end) + relativedelta(days=-reprocess_range)).date())

spark.conf.set("parameters.range_start", range_start)
spark.conf.set("parameters.range_end",   range_end)
print(f"Rango: {range_start} → {range_end}")
```

---

### 2. Lectura y Ajuste de Clasificación  
```python
from pyspark.sql import functions as F

df = spark.sql("SELECT * FROM db_bronze.VW_UP_SEL_ANALISIS_VENTAS")
updated_df = df.withColumn(
  "clasificacion",
  F.when(F.col("nombre_grupo").like("%INGRESO ADICIONAL X SERVICIOS%"),
         "UTILIDAD SERVICIOS")
   .otherwise(F.col("clasificacion"))
)
updated_df.createOrReplaceTempView("updated_VW_UP_SEL_ANALISIS_VENTAS")
```

---

### 3. Pre-vista (`VW_UP_SEL_AV_VENTAS_pre`)  
```sql
CREATE OR REPLACE TEMPORARY VIEW VW_UP_SEL_AV_VENTAS_pre AS (
  SELECT
    to_date(fecha_emision) AS fecha_emision,
    mes, year, id_file, id_tipo_de_comprobante,
    numero_serie1, id_factura_cabeza, id_sucursal,
    descripcion, id_punto_emision, punto_emision_descrip,
    id_cliente, nombre_cliente, id_subcodigo,
    subcodigo_descripcion, tipo_cliente_descrip,
    division, unidad_negocio, vendedor_file_nombre,
    moneda_srv, gravado, igv, inafecto, no_gravado,
    srv_recargo_consumo, descuento, venta_neta,
    tipo_de_cambio,
    -- Cálculo de totales
    (gravado + igv + inafecto + no_gravado + srv_recargo_consumo) AS venta_total,
    CASE WHEN moneda_srv = 'USD' THEN venta_total
         ELSE venta_total / tipo_de_cambio END AS venta_total_usd,
    CASE WHEN moneda_srv = 'USD' THEN costo_con_igv
         ELSE costo_con_igv / tipo_de_cambio END AS costo_total_usd,
    (venta_total_usd - costo_total_usd) AS utilidad_total,
    utilidad_total / 1.18 AS utilidad_neta_usd,
    -- Utilidades parciales
    CASE WHEN clasificacion = 'UTILIDAD SERVICIOS' THEN venta_neta_usd ELSE 0 END
      + CASE WHEN clasificacion = 'FEE SERVICIOS' THEN venta_neta_usd ELSE 0 END
      AS uti_fee_comisiones,
    CASE WHEN id_orden_de_servicio > 1 THEN utilidad_neta_usd ELSE 0 END AS uti_servicios_usd,
    (uti_fee_comisiones + uti_servicios_usd) AS uti_neta_total_fee_svs,
    -- Canales y segmentación
    listaUnCC.UN AS UN,
    listaUnCC.CC AS CC,
    FECHAS.week_monday AS num_semana_calendario,
    CONCAT(
      date_format(date_sub(fecha_emision, (dayofweek(fecha_emision)+5)%7),'ddMMM'),
      '-',
      date_format(date_add(fecha_emision,(7-dayofweek(fecha_emision)+1)%7),'ddMMM')  
    ) AS rango_semana_calendario  
  FROM updated_VW_UP_SEL_ANALISIS_VENTAS av
  LEFT JOIN db_bronze.file_listas_av_semanal_listaUnYCcs listaUnCC  
    ON listaUnCC.cliente = db_parameters.udf_decrypt(av.nombre_cliente)  
  LEFT JOIN FECHAS  
    ON av.fecha_emision = FECHAS.fecha  
  WHERE DATE(fecha_emision) BETWEEN '${parameters.range_start}'  
    AND '${parameters.range_end}'
);
```

---

### 4. Vista Limpia (`VW_UP_SEL_AV_VENTAS`)  
```sql
CREATE OR REPLACE TEMPORARY VIEW VW_UP_SEL_AV_VENTAS AS (
  SELECT
    fecha_emision, id_file, UN, CC,
    venta_neta, venta_total, venta_total_usd,
    utilidad_total, utilidad_neta_usd,
    uti_fee_comisiones, uti_servicios_usd, uti_neta_total_fee_svs,
    CASE  
      WHEN punto_emision_descrip = 'CONTACT CENTER B2C' THEN 'CONTACT CENTER'  
      WHEN upper(CC) = 'CONTACT CENTER' THEN 'CONTACT CENTER'  
      ELSE como_se_entero_descrip  
    END AS como_se_entero_descrip2,  
    num_semana_calendario, rango_semana_calendario  
  FROM VW_UP_SEL_AV_VENTAS_pre  
);
```

---

### 5. Detección de Anomalías Semanales  
```sql
-- Ventas negativas  
CREATE OR REPLACE TEMPORARY VIEW venta_av_semanal AS (  
  SELECT  
    id_file,  
    SUM(venta_neta_usd) AS total_venta_neta_usd  
  FROM VW_UP_SEL_AV_VENTAS  
  WHERE clasificacion IN ('FEE SERVICIOS','UTILIDAD SERVICIOS','VENTA SERVICIOS')  
  GROUP BY id_file  
  HAVING total_venta_neta_usd < 0  
);  

-- Utilidades negativas  
CREATE OR REPLACE TEMPORARY VIEW uti_av_semanal AS (  
  SELECT  
    id_file,  
    SUM(uti_neta_total_fee_svs) AS total_uti_neta_total_fee_svs  
  FROM VW_UP_SEL_AV_VENTAS  
  WHERE clasificacion IN ('FEE SERVICIOS','UTILIDAD SERVICIOS','VENTA SERVICIOS')  
  GROUP BY id_file  
  HAVING total_uti_neta_total_fee_svs < 0  
);  
```

---

### 6. Pre-Final (`_pre`)  
```sql
CREATE OR REPLACE TEMPORARY VIEW VW_md_tb_emisivo_b2c_ventasservicios_detalle_pre AS (  
  SELECT  
    up_sel.* EXCEPT(venta_neta, venta_total_usd, uti_neta_total_fee_svs),  
    COALESCE(venta.total_venta_neta_usd, up_sel.venta_neta)            AS venta_neta,  
    COALESCE(venta.total_venta_total_usd, up_sel.venta_total_usd)     AS venta_total_usd,  
    COALESCE(uti_av_semanal.total_uti_neta_total_fee_svs, up_sel.uti_neta_total_fee_svs)  
      AS uti_neta_total_fee_svs  
  FROM VW_UP_SEL_AV_VENTAS up_sel  
  LEFT JOIN venta_av_semanal     venta       ON up_sel.id_file = venta.id_file  
  LEFT JOIN uti_av_semanal       uti_av_semanal  
    ON up_sel.id_file = uti_av_semanal.id_file  
);
```

---

### 7. Reglas Finales  
```sql
CREATE OR REPLACE TEMPORARY VIEW VW_md_tb_emisivo_b2c_ventasservicios_detalle AS (  
  SELECT  
    s.*,
    CASE  
      WHEN s.UN = 'Retail'    THEN COALESCE(  
        CASE  
          WHEN s.id_cliente = 79296 AND date_format(s.fecha_emision,'yyyyMM')='202410'  
            THEN 'San Miguel'  
          WHEN s.vendedor_file_nombre IN (  
            'Brenda Pinares Osis',...  
          ) OR s.como_se_entero_descrip LIKE 'WHATSAPP CC'  
            THEN 'CC Retail'  
          ELSE NULL  
        END, m.Tienda  
      )  
      WHEN s.UN = 'Web'       THEN CASE  
        WHEN s.vendedor_file_nombre LIKE 'Motor%' THEN 'Web'  
        ELSE 'CC Web'  
      END  
      WHEN s.UN = 'Corporate' THEN coalesce(tc.TipoCliente,'Regular')  
      ELSE 'ND'  
    END AS negocio,  

    CASE  
      WHEN s.UN = 'Retail' AND s.subcodigo_descripcion LIKE '%OUTLET%' THEN 'SIT'  
      WHEN s.UN = 'Retail' AND s.subcodigo_descripcion LIKE '%CIERRA%'  THEN 'CIERRA PUERTA'  
      ELSE 'NORMAL'  
    END AS evento,  

    CASE  
      WHEN s.UN = 'Web' AND s.como_se_entero_descrip LIKE 'WHATSAPP%' THEN 'CHAT'  
      WHEN s.UN = 'Web' AND s.fecha_emision >= '2024-09-24'            THEN 'CHAT'  
      ELSE 'Otros'  
    END AS equipo,  

    CASE  
      WHEN s.UN = 'Corporate' AND s.unidad_negocio LIKE '%VENTAS DE INCENTIVO%' THEN 'MICE'  
      WHEN s.UN = 'Corporate' THEN 'CORPORATE'  
      ELSE s.UN  
    END AS sub_un  

  FROM VW_md_tb_emisivo_b2c_ventasservicios_detalle_pre s  
  LEFT JOIN data_bi.db_bronze.file_maestro_asesor_b2c m  
    ON date_format(s.fecha_emision,'yyyyMM') = m.Periodo  
   AND s.vendedor_file_nombre = m.Asesor  
  LEFT JOIN TipoClientes_Corpo tc  
    ON s.id_cliente = tc.id_cliente  
);
```

---

### 8. Carga Incremental 🔄  
```python
results = [create_insert_incremental_table(  
  'VW_md_tb_emisivo_b2c_ventasservicios_detalle',  
  'fecha_emision',  
  range_start, range_end,  
  'db_silver'  
)]  
display(results)  
check_errors(results)  
```
