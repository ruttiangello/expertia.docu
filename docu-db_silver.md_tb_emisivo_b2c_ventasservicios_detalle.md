## `md_tb_emisivo_b2c_ventasservicios_detalle`

### 🔎 Propósito  
Consolida **transacciones de servicios emisivos** a nivel detalle para:
- **B2C** (Retail + Web)  
- **B2B Corporate**  
Incluye ventas de servicios (Journey Assist / TA) y comisiones asociadas, listo para análisis y reporting.

### 💰 Campos de Monto  
| Campo               | Descripción                                                              |
| ------------------- | ------------------------------------------------------------------------ |
| **venta_neta**      | Importe neto de servicios vendidos (sin impuestos ni recargos).         |
| **venta_total**     | Monto bruto (gravado + igv + inafecto + no_gravado + srv_recargo_consumo). |
| **venta_neta_usd**  | Venta neta convertida a USD.                                            |
| **venta_total_usd** | Venta total convertida a USD.                                           |
| **costo_total_usd** | Costo de servicio con IGV en USD.                                       |
| **utilidad_total**  | Venta_total_usd – costo_total_usd.                                       |
| **utilidad_neta_usd** | Utilidad después de impuestos (utilidad_total ÷ 1.18).               |
| **uti_fee_comisiones** | Utilidad atribuible a fees y comisiones.                           |
| **uti_servicios_usd** | Utilidad específica de servicios (solo si id_orden_de_servicio >1).  |
| **uti_neta_total_fee_svs** | Suma de uti_fee_comisiones + uti_servicios_usd.               |

### 📂 Ubicación de la Lógica  
Toda la transformación y reglas están en el notebook SQL:  03_reglas_de_negocio_up_sel_analisis_de_ventas_srv


---

## 📝 Resumen de Secciones

1. **Inicialización & Parámetros**  
   - Carga de utilitarios (`global_parameter_py`, `00_util_ingenieria_py`)  
   - Configuración Spark:  
     - `spark.databricks.io.cache.enabled = True` (acelera lecturas)  
     - `spark.sql.shuffle.partitions = auto` (optimiza shuffles)  
     - `USE {target_catalog}.db_bronze` (esquema Bronze)  
   - Widgets:  
     - `range_start`, `range_end`, `reprocess_range` (por defecto 65 días)  
     - Si faltan fechas, calcula valores por defecto:  
       - `range_end` = hoy  
       - `range_start` = `range_end` − `reprocess_range`  
   - Guarda `parameters.range_start` y `parameters.range_end` en `spark.conf`  

2. **Lectura y Ajuste de Clasificación**  
   - Lee vista base `VW_UP_SEL_ANALISIS_VENTAS` desde Bronze  
   - Sobrescribe `clasificacion = 'UTILIDAD SERVICIOS'` cuando `nombre_grupo` contiene “INGRESO ADICIONAL X SERVICIOS”  

3. **Construcción de `VW_UP_SEL_AV_VENTAS_pre`**  
   - Selección y normalización de campos:  
     - Fechas (`to_date(fecha_emision)`), facturación, cliente, subcódigos, punto de emisión…  
   - Cálculo de montos y conversiones:  
     - `venta_total` = `gravado + igv + inafecto + no_gravado + srv_recargo_consumo`  
     - `venta_total_usd` y `costo_total_usd` según `tipo_de_cambio`  
     - `utilidad_total` = `venta_total_usd – costo_total_usd`  
     - `utilidad_neta_usd` = `utilidad_total ÷ 1.18`  
   - Métricas de utilidad parcial:  
     - `uti_fee_comisiones` (fees + comisiones)  
     - `uti_servicios_usd` (solo si `id_orden_de_servicio > 1`)  
     - `uti_neta_total_fee_svs` = suma de ambos  
   - Asignación de canales (`UN`) y segmento (`CC`):  
     - JOIN con `file_listas_av_semanal_listaUnYCcs`  
     - Reglas especiales (WhatsApp Web → Web, “CUENTAS COMERCIALES” → Corporate/Mice…)  
   - Enriquecimiento con `FECHAS`:  
     - `num_semana_calendario`, `rango_semana_calendario`, `num_semanal`, `rango_semanal`  
     - `segmentacion_cliente` desde tabla externa  

4. **Transformación Final `VW_UP_SEL_AV_VENTAS`**  
   - Refina `como_se_entero_descrip2` para unificar etiquetas (CHAT, CITA VIRTUAL, LLAMADA TELEFONICA…)  
   - Unifica casos de “CONTACT CENTER” vs otros  
   - Conserva solo campos clave para el análisis final  

5. **Detección de Anomalías Semanales**  
   - **`venta_av_semanal`**: agrupa ventas negativas (`SUM(venta_neta_usd) < 0`) por `id_file`  
   - **`uti_av_semanal`**: agrupa utilidades negativas (`SUM(uti_neta_total_fee_svs) < 0`) por `id_file`  

6. **Pre-Final (`_pre`)**  
   - Crea `VW_md_tb_emisivo_b2c_ventasservicios_detalle_pre`  
   - **COALESCE** entre valores originales y valores ajustados de `venta_av_semanal` / `uti_av_semanal`  
   - Reemplaza solo los montos que presentaron anomalías negativas  

7. **Reglas de Negocio Finales**  
   - Crea `VW_md_tb_emisivo_b2c_ventasservicios_detalle`  
   - Lógica por `UN`:  
     - **Negocio**: Retail, Web, Corporate (según cliente y `como_se_entero_descrip`)  
     - **Evento**: SIT, CIERRA PUERTA, NORMAL según subcódigo  
     - **Equipo**: CHAT, MOTOR SVS, “Otros” según fecha y vendedor  
     - **Sub-UN**: Corporate → MICE vs CORPORATE, resto mantiene `UN`  
   - JOIN con `file_maestro_asesor_b2c` y `TipoClientes_Corpo`  

8. **Carga Incremental 🔄**  
   - Llama a  
     ```python
     create_insert_incremental_table(
       'VW_md_tb_emisivo_b2c_ventasservicios_detalle',
       'fecha_emision',
       range_start, range_end,
       'db_silver'
     )
     ```  
   - Procesa solo el rango de fechas definido en los widgets, optimizando tiempo y recursos.  

---
