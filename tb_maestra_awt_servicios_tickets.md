# 📘 Documentación: AWT Servicios y Tickets (Silver)

Este documento describe la lógica y propósito de las tablas **`tb_maestra_awt_servicio_agrupado_diario`** y **`tb_maestra_awt_ticket_agrupado_diario`** en el esquema `data_bi.db_silver`.  
Las transformaciones se encuentran en el workspace **`proceso_silver_awt_servicios_tickets`**.

---

## 🔎 Propósito

- **`tb_maestra_awt_ticket_agrupado_diario`**  
  Consolida los tickets AWT y OTA AWT a nivel transaccional, extrayendo ciudad de origen y destino, codificando IATA y normalizando rutas para análisis de volúmenes y segmetnación de rutas.

- **`tb_maestra_awt_servicio_agrupado_diario`**  
  Agrupa las transacciones de servicios AWT (Journey Assist, seguros y anexos), eliminando duplicados y preparando los datos para su incorporación en capas superiores.

---

## 📝 Resumen de Secciones

1. **Inicialización & Parámetros**  
   - Se obtiene `target_catalog` desde Azure Key Vault.  
   - Se habilita caché I/O y particiones automáticas en Spark para optimizar rendimiento.  

2. **Procesamiento de Tickets**  
   - **Lectura raw:**  
     ```python
     spark.sql("SELECT DISTINCT ticket_number, nombre_ciudades FROM db_bronze.tb_raw_awt_ticket_diario")
     ```  
   - **Extracción Origen/Destino:**  
     - Se define una UDF (`obtener_origen_destino`) que parsea la cadena de ciudades y devuelve la ciudad de origen y la de destino, detectando viajes de ida y vuelta simétricos o lineales.  
   - **Unión y limpieza:**  
     - Se une la UDF al raw, se añaden columnas `ciudad_origen` y `ciudad_destino`.  
     - Se recuperan todos los campos originales y se eliminan duplicados.  
   - **Codificación IATA:**  
     - Se une con el catálogo de ciudades (`file_catalogo_ciudades`) para mapear nombres de ciudad a códigos IATA (`codciudad`).  
     - Se genera la columna `o_d_iata` como `ORIGEN->DESTINO`.  
   - **Normalización de rutas:**  
     - Se calcula `ms_ruta` ordenando alfabéticamente los tramos, asegurando consistencia (`LIM` siempre al inicio cuando aplica).  
   - **Persistencia:**  
     ```python
     df_ticket_final.write.mode("overwrite").saveAsTable(f"{target_catalog}.db_silver.tb_maestra_awt_ticket_agrupado_diario")
     spark.sql("REFRESH TABLE ...")
     ```

3. **Procesamiento de Servicios**  
   - **Lectura raw y limpieza:**  
     ```python
     df_servicios = spark.sql("SELECT * FROM db_bronze.tb_raw_awt_servicio_diario")
     df_servicios_sin_ciudad = df_servicios.drop("ciudad_origen","ciudad_destino")
     df_servicios_agrupado = df_servicios_sin_ciudad.dropDuplicates()
     ```  
   - **Persistencia:**  
     ```python
     df_servicios_agrupado.write.mode("overwrite").saveAsTable(f"{target_catalog}.db_silver.tb_maestra_awt_servicio_agrupado_diario")
     spark.sql("REFRESH TABLE ...")
     ```

4. **Carga Completa**  
   - Ambas tablas se regeneran por completo en cada ejecución, ya que se usan `overwrite` + `REFRESH TABLE` sin lógica incremental.

---

## 🔗 Ubicación de la Lógica

Notebook PySpark:  
```
proceso_silver_awt_servicios_tickets
```

Aquí se agrupan y limpian tanto tickets como servicios AWT, preparándolos para su uso en la capa Silver de Databricks.

---

*Fin de la documentación.*