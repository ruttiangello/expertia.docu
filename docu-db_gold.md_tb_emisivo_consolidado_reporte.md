# 📘 Documentación | db_gold.md_tb_emisivo_consolidado_reporte

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

---

## Introducción
Este documento ofrece una **visión detallada** del pipeline de ventas emisivas en Expertia:
- Desde la ingestión de datos raw (CSV, tablas Silver) en **Bronze**.
- Transformaciones y agregaciones por segmento en **Silver**.
- Consolidación y publicación de la tabla final en **Gold**.

---

## Arquitectura Medallion

```text
Bronze 🥉  -->  Silver 🪙  -->  Gold 🥇
+ Raw CSV         + Limpieza y         + Data Marts consolidados
+ Vistas temporales  enriquecimiento       + Tablas Delta listas para BI
```
- **Bronze**: ingesta raw; sin transformaciones complejas.
- **Silver**: limpieza, estandarización y segmentación (df1…df6).
- **Gold**: tabla final única `md_tb_emisivo_consolidado_reporte`.

---

## Inicialización de Ambiente
```python
# 🔑 Leer nombre de catálogo desde Azure Key Vault
target_catalog = dbutils.secrets.get('kv-dbi', 'default-catalog')

# 🧰 Import de funciones SQL de Spark
import pyspark.sql.functions as f
```
- Permite apuntar a `dev` o `prod` sin cambiar código.
- `f` simplifica llamadas a funciones (`f.col`, `f.sum`, etc.).

---

## Capa Bronze: Ingestión de Datos
```python
# 📂 Carga CSV como DataFrame y crea vista temporal
csv_sources = [
    ("/Volumes/data_bi/db_bronze/files_analistas/Lista_Grupo_202406.csv", "dotacion_svs_202406", ","),
    ("/Volumes/data_bi/db_bronze/files_analistas/file_cartera_b2b_202502.csv", "file_cartera_b2b_202502", ","),
    ("/Volumes/data_bi/db_bronze/files_analistas/TipoClientes_Corpo.csv", "TipoClientes_Corpo", ","),
    ("/Volumes/data_bi/db_bronze/files_analistas/GrupoDestino_SVS.csv", "GrupoDestino_SVS", ","),
    ("/Volumes/data_bi/db_bronze/files_analistas/Budget_Emisivo.csv", "Budget_Emisivo", "|"),
    ("/Volumes/data_bi/db_bronze/files_analistas/Maestro_Cadena_Proveedor.csv", "Cadena_Proveedor", ",")
]

for path, view, sep in csv_sources:
    df = spark.read.format("csv")         .option("header", "true")         .option("sep", sep)         .option("inferSchema", "true")         .load(path)
    df.createOrReplaceTempView(view)
```

---

## Capa Silver: Transformaciones por Segmento

### 1. Resumen Boletos (df1)
```python
# ✈️ Boletos Aéreo y Asistencia reales
# Fuente: db_silver.md_tb_emisivo_ventasboletos_detalle
# - Filtros: año >= 2023, tkt_valido = 'Si', productos en ('Aereo','Asistencia'), ...
# - Dimensiones: fechas, geografía, canal (UN, SUB_UN), cliente desencriptado
# - Métricas: VentaNeta, VentaFull, Q_tkts, Q_files, Fee, Proy_Uti, etc.
```

### 2. SVS B2B Servicios (df2)
```python
# 🏢 Servicios B2B reales
# Fuente: db_silver.md_tb_emisivo_b2b_ventasservicios_detalle
# - UDF decrypt en s.cliente_documento y s.nom_cliente
# - Filtros: tipo_producto = 'Servicios', estado_item_adm = 'E'
# - Métricas: Venta_Neta_Comercial, VENTA, count_pax, Fee, Proy_Uti, Costo_os
```

### 3. SVS B2C Servicios (df3)
```python
# 🏠 Servicios B2C (Web, Retail, Corporate)
# Fuente: db_silver.md_tb_emisivo_b2c_ventasservicios_detalle
# - UDF decrypt en b2c.nombre_cliente y cli.DOCUMENTO
# - Filtros: UN in ('Web','Retail','Corporate'), rangos de venta, id_file excluidos
# - Métricas: venta_neta_usd, venta_total_usd, Fee, Proy_Uti
```

### 4. Budget Emisivo (df4)
```python
# 💸 Presupuesto vs real
# Fuente: Bronze view Budget_Emisivo
# - Convierte fechas: Emision, Salida +1 mes, Retorno +5 días
# - Asigna canal UN/SUB_UN, Cartera_Negocio
# - Métricas: Volumen, Fee_Bol, Descuento, Com_Std, Proy_Uti
```

### 5. AWT Tickets (df5)
```python
# ✈️ Tickets AWT & OTAs
# Fuente: db_silver.tb_maestra_awt_ticket_agrupado_diario
# - Normaliza O_D_iata, filtra clasificacion_fee 'Sin fee'
# - Agrupa por booking_code y ota para asignar UN/SUB_UN
# - Métricas: VentaNeta, VentaFull, Q_tkts, Q_files, Proy_Uti
```

### 6. AWT Servicios (df6)
```python
# 🛎️ Servicios AWT
# Fuente: db_silver.tb_maestra_awt_servicio_agrupado_diario
# - Misma estructura que df5, con zona_dest ...
```

---

## Consolidación y Tipado
```python
# 🔗 Union de todos los segments
Consolidado = df1.union(df2).union(df3).union(df4).union(df5).union(df6)

# 💎 Cast de VentaFull a decimal(38,15)
Consolidado2 = Consolidado.withColumn("VentaFull", f.col("VentaFull").cast("decimal(38,15)"))
```

---

## Capa Gold: Escritura y Refresh
```python
# 💾 Sobrescribe Gold
Consolidado2.write.mode("overwrite").saveAsTable(f"{target_catalog}.db_gold.md_tb_emisivo_consolidado_reporte")

# 🔄 Refresca metacache
spark.sql(f"REFRESH TABLE {target_catalog}.db_gold.md_tb_emisivo_consolidado_reporte")
```
