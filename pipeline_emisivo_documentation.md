# 📘 Documentación del Pipeline Emisivo en Databricks

## Tabla de Contenidos
1. [Introducción](#introducción)
2. [Arquitectura Medallion](#arquitectura-medallion)
3. [Inicialización de Ambiente](#inicialización-de-ambiente)
4. [Capa Bronze: Ingestión de Datos](#capa-bronze-ingestión-de-datos)
5. [Capa Silver: Transformaciones por Segmento](#capa-silver-transformaciones-por-segmento)
   - [1. Resumen Boletos (df1)](#1-resumen-boletos-df1)
   - [2. SVS B2B Servicios (df2)](#2-svs-b2b-servicios-df2)
   - [3. SVS B2C Servicios (df3)](#3-svs-b2c-servicios-df3)
   - [4. Budget Emisivo (df4)](#4-budget-emisivo-df4)
   - [5. AWT Tickets (df5)](#5-awt-tickets-df5)
   - [6. AWT Servicios (df6)](#6-awt-servicios-df6)
6. [Consolidación y Tipado](#consolidación-y-tipado)
7. [Capa Gold: Escritura y Refresh](#capa-gold-escritura-y-refresh)
8. [Buenas Prácticas y Tips](#buenas-prácticas-y-tips)
9. [Anexo: Cheat Sheet SQL](#anexo-cheat-sheet-sql)

---

## Introducción
Este documento describe el pipeline completo de **ventas emisivas** en Expertia, desde la ingestión de datos brutos hasta la tabla final `md_tb_emisivo_consolidado_reporte`, lista para reporting en Power BI y otros dashboards. Está pensado para compartir con el equipo de BI y mantener claridad en cada paso. 🚀

---

## Arquitectura Medallion

🥉 **Bronze**: Datos crudos (CSV, tables silver) ingresados sin modificaciones.

🪙 **Silver**: Datos limpiados, transformados y divididos por segmento (df1…df6).

🥇 **Gold**: Data Marts consolidados con métricas agregadas listas para consumo.

---

## Inicialización de Ambiente
```python
# 🔑 Obtiene dinámicamente el catálogo (dev/prod) desde Azure Key Vault
target_catalog = dbutils.secrets.get('kv-dbi', 'default-catalog')

# 🧰 Import de funciones SQL de Spark
import pyspark.sql.functions as f
```

---

## Capa Bronze: Ingestión de Datos
```python
# 📂 Carga CSV como DataFrame y crea vista temporal
for (path, view, sep) in [
  (".../Lista_Grupo_202406.csv", "dotacion_svs_202406", ','),
  (".../file_cartera_b2b_202502.csv", "file_cartera_b2b_202502", ','),
  (".../TipoClientes_Corpo.csv", "TipoClientes_Corpo", ','),
  (".../GrupoDestino_SVS.csv", "GrupoDestino_SVS", ','),
  (".../Budget_Emisivo.csv", "Budget_Emisivo", '|'),
  (".../Maestro_Cadena_Proveedor.csv", "Cadena_Proveedor", ',')
]:
    df = spark.read.format("csv") \
        .option("header","true") \
        .option("sep", sep) \
        .option("inferSchema","true") \
        .load(path)
    df.createOrReplaceTempView(view)  # Vista Bronze para Silver
```

---

## Capa Silver: Transformaciones por Segmento

### 1. Resumen Boletos (df1)
```python
# ✈️ Boletos Aéreo y Asistencia reales
# df1: md_tb_emisivo_ventasboletos_detalle
# Filtra por año, validez, canal y categorías, agrupa y suma métricas.
```

### 2. SVS B2B Servicios (df2)
```python
# 🏢 Servicios B2B reales
# df2: md_tb_emisivo_b2b_ventasservicios_detalle
# Desencripta cliente, agrupa por cotización, suma netas y comisiones.
```

### 3. SVS B2C Servicios (df3)
```python
# 🏠 Servicios B2C (Web, Retail, Corporate)
# df3: md_tb_emisivo_b2c_ventasservicios_detalle
# UDF decrypt, filtra UN, suma servicios y fees.
```

### 4. Budget Emisivo (df4)
```python
# 💸 Presupuesto vs real
# df4: Budget_Emisivo (pipe-separated)
# Convierte fechas presupuestadas, asigna UN/SUB_UN y métricas del plan.
```

### 5. AWT Tickets (df5)
```python
# ✈️ Tickets AWT & OTAs
# df5: tb_maestra_awt_ticket_agrupado_diario
# Normaliza O-D, filtra sin fee, agrupa por booking_code.
```

### 6. AWT Servicios (df6)
```python
# 🛎️ Servicios AWT
# df6: tb_maestra_awt_servicio_agrupado_diario
# Mismas dimensiones que tickets, con zona ND y sumatorias de fares.
```

---

## Consolidación y Tipado
```python
# 🔗 Union de todos los segmentos
Consolidado = df1.union(df2).union(df3).union(df4).union(df5).union(df6)

# 💎 Asegura alta precisión en la métrica de venta
Consolidado2 = Consolidado.withColumn(
    "VentaFull", f.col("VentaFull").cast("decimal(38,15)")
)
```

---

## Capa Gold: Escritura y Refresh
```python
# 💾 Sobrescribe tabla Gold con el snapshot consolidado
Consolidado2.write.mode("overwrite").saveAsTable(
    f"{target_catalog}.db_gold.md_tb_emisivo_consolidado_reporte"
)

# 🔄 Refresca el metacache para asegurar lecturas actualizadas
spark.sql(f"REFRESH TABLE {target_catalog}.db_gold.md_tb_emisivo_consolidado_reporte")
```

---

## Buenas Prácticas y Tips
- 📝 Documenta cada sección en tu notebook con comentarios claros.
- 📊 Usa `LIMIT` o `.sample()` durante desarrollo para no saturar el clúster.
- 🔒 Gestiona accesos con Unity Catalog y controla permisos SQL.
- 📆 Programa tareas de refresco y orquestación con Databricks Jobs.

---

## Anexo: Cheat Sheet SQL
- Consulta total, agrupaciones, joins, window functions, DDL/DML, Delta optimizations.
- 👉 Copia tu cheat sheet en el README para fácil referencia.

> ¡Listo! Con esta documentación tendrás un **manual de referencia completo** para entender, mantener y extender tu pipeline emisivo en Databricks. 🎉
