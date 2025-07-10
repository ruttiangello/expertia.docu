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

## 📝 Resumen de Secciones (detalle ampliado)

1. **Inicialización & Parámetros**  
   - **Carga de utilitarios** (`global_parameter_py`, `00_util_ingenieria_py`)  
   - **Configuración Spark**  
     - `cache.enabled = True` para acelerar lecturas  
     - `shuffle.partitions = auto` para optimización de shuffles  
     - `USE {target_catalog}.db_bronze` ⇒ esquema Bronze por defecto  
   - **Widgets de fecha**  
     - `range_start`, `range_end` y `reprocess_range`  
     - Lógica de defaults: si no hay widget, usa “hoy” y retrocede `reprocess_range` días (por defecto 65)  
     - Guarda `parameters.range_start/end` en `spark.conf`

2. **Lectura y Ajuste de Clasificación**  
   - Lee la vista base **`VW_UP_SEL_ANALISIS_VENTAS`** (raw de ventas srv)  
   - Sobrepone `clasificacion = 'UTILIDAD SERVICIOS'` cuando `nombre_grupo` contiene “INGRESO ADICIONAL X SERVICIOS”

3. **Construcción de `VW_UP_SEL_AV_VENTAS_pre`**  
   - **Selección y normalización** de campos clave:  
     - Fechas (`to_date(fecha_emision)`), facturación, cliente, punto de emisión, subcódigos…  
   - **Montos y conversiones**:  
     - `venta_total` = suma de `gravado + igv + inafecto + no_gravado + srv_recargo_consumo`  
     - `venta_total_usd`, `costo_total_usd` según `tipo_de_cambio`  
     - `utilidad_total` = `venta_total_usd – costo_total_usd`  
     - `utilidad_neta_usd` = `utilidad_total ÷ 1.18`  
   - **Cálculo de utilidades parciales**:  
     - `uti_fee_comisiones`  (fees + comisiones)  
     - `uti_servicios_usd`   (solo para órdenes de servicio >1)  
     - `uti_neta_total_fee_svs` = suma de ambas  
   - **Asignación de canales y unidades de negocio**:  
     - JOIN con `file_listas_av_semanal_listaUnYCcs` para obtener `UN` y `CC` por cliente  
     - Casos especiales (WhatsApp Web → Web, directos → Retail, “CUENTAS COMERCIALES” → Corporate / Mice…)  
   - **Enriquecimiento temporal**:  
     - JOIN a calendario `FECHAS` ⇒ `num_semana_calendario`, `rango_semana_calendario`, `num_semanal`, `rango_semanal`  
     - `segmentacion_cliente` desde tabla de segmentación externa

4. **Transformación final `VW_UP_SEL_AV_VENTAS`**  
   - Refina `como_se_entero_descrip2` para unificar etiquetas de contacto (CHAT, CITA VIRTUAL, LLAMADA TELEFONICA…)  
   - Ajusta casos especiales de “CONTACT CENTER” y presencia de “Web”  
   - Conserva únicamente los campos necesarios para el análisis

5. **Detección de Anomalías Semanales**  
   - **`venta_av_semanal`**  
     - Agrupa por `id_file` las ventas (`venta_neta`, `venta_total`, `venta_neta_usd`)  
     - Filtra solo las que tienen sumatorio USD < 0 ⇒ valores negativos inusuales  
   - **`uti_av_semanal`**  
     - Agrupa por `id_file` las utilidades (`uti_neta_total_fee_svs`, `uti_servicios_usd`, `utilidad_neta_usd`, `uti_fee_comisiones`)  
     - Filtra sumas negativas para corregir errores de cálculo

6. **Pre-​final: `VW_md_tb_emisivo_b2c_ventasservicios_detalle_pre`**  
   - Toma los registros de `VW_UP_SEL_AV_VENTAS_pre`  
   - **COALESCE** entre valores originales y valores “venta_av_semanal” o “uti_av_semanal”  
     - Reemplaza sólo los que arrojaron anomalías negativas  
   - Resultado: montos “limpios” listos para reglas de negocio finales

7. **Aplicación de Reglas de Negocio Finales**  
   - Vista **`VW_md_tb_emisivo_b2c_ventasservicios_detalle`**:  
     - **Determina `negocio`** (Retail, Web, Corporate) con lógica por `UN`, cliente y `como_se_entero_descrip`  
     - **Clasifica `evento`** (SIT, CIERRA PUERTA, NORMAL) según subcódigo y canal  
     - **Asigna `equipo`** (CHAT vs recuperación vs MOTOR SVS vs etc.) según fecha y vendedor  
     - **Define `sub_un`** para Corporate / Mice vs resto  
     - JOIN con `TipoClientes_Corpo` para Corporate y `file_maestro_asesor_b2c` para Retail

8. **Carga Incremental**  
   - Llamada a  
     ```python
     create_insert_incremental_table(
       'VW_md_tb_emisivo_b2c_ventasservicios_detalle',
       'fecha_emision',
       range_start, range_end, 'db_silver'
     )
     ```  
   - Solo procesa el rango de fechas definido en los widgets, optimizando tiempo y recursos.
---
