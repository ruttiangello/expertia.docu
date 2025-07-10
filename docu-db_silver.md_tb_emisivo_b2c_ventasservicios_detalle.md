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
   - Carga de `global_parameter_py`, utilitarios de ingeniería  
   - Configura cache I/O, particiones automáticas y base Bronze  
   - Widgets para `range_start`, `range_end`, `reprocess_range`

2. **Lectura y Ajuste de Clasificación**  
   - Lee `VW_UP_SEL_ANALISIS_VENTAS` desde Bronze  
   - Sobrepone `clasificacion = 'UTILIDAD SERVICIOS'` para nombre_grupo específico

3. **Vista Previa (`VW_UP_SEL_AV_VENTAS_pre`)**  
   - Selecciona y normaliza campos de factura, cliente, monto (gravado, igv, inafecto, descuento…)  
   - Calcula `venta_total`, `venta_total_usd`, `costo_total_usd`, `utilidad_total` y métricas de utilidad  
   - Asigna unidad de negocio (`UN`), canal (`CC`), sub-UN y demás flags de negocio  
   - Enriquecimientos con catálogos (`listaUnCC`, `FECHAS`)

4. **Vista Limpia (`VW_UP_SEL_AV_VENTAS`)**  
   - Refina `como_se_entero_descrip` a títulos estandarizados (CHAT, CITA VIRTUAL…)  
   - Limpia duplicados de canales de entrada  
   - Mantiene solo los campos finales de análisis

5. **Detección de Anomalías Semanales**  
   - **`venta_av_semanal`**: agrupa ventas negativas (`venta_neta_usd`) por `id_file`  
   - **`uti_av_semanal`**: agrupa utilidades netas negativas (`uti_neta_total_fee_svs`)  

6. **Pre-Final (`VW_md_tb_emisivo_b2c_ventasservicios_detalle_pre`)**  
   - Coalesce: si hay anomalía negativa, reemplaza los montos originales por los valores ajustados de `venta_av_semanal` y `uti_av_semanal`

7. **Final (`VW_md_tb_emisivo_b2c_ventasservicios_detalle`)**  
   - Agrega lógica de negocio específica de Retail, Web y Corporate:  
     - Determina `negocio`, `evento`, `equipo`, `sub_un` según `UN` y reglas de puntos de venta  
   - Une con catálogo `TipoClientes_Corpo` para Corporate

8. **Carga Incremental**  
   - Invoca `create_insert_incremental_table('VW_md_tb_emisivo_b2c_ventasservicios_detalle', 'fecha_emision', ...)`  
   - Procesa solo el rango de fechas definido en los widgets  

---
