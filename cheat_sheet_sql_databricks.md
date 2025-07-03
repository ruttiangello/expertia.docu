# Cheat Sheet: SQL en Databricks

## 1. Ventas y Montos

**Ventas Totales (SUM)**
```sql
SELECT canal, SUM(venta_monto) AS ventas_totales
FROM db_silver.tb_ventas
GROUP BY canal;
```
*Descripción:* Suma todos los montos de venta por canal.  
*Valor:* Permite comparar ingresos por canal y priorizar estrategias.

**Ticket Promedio (AVG)**
```sql
SELECT producto, AVG(venta_monto) AS ticket_promedio
FROM db_silver.tb_ventas
GROUP BY producto;
```
*Descripción:* Calcula el ingreso medio por transacción.  
*Valor:* Revela comportamientos de compra; útil para diseñar bundles o promociones.

## 2. Crecimiento y Variación

**CAGR**
```sql
SELECT 
  POWER(ventas_final/ventas_inicio, 1.0/periodos) - 1 AS cagr
FROM (
  SELECT
    SUM(CASE WHEN fecha = '2023-12-31' THEN monto END) AS ventas_inicio,
    SUM(CASE WHEN fecha = '2024-12-31' THEN monto END) AS ventas_final,
    1 AS periodos
  FROM db_silver.tb_ventas
);
```
*Descripción:* Calcula la tasa de crecimiento compuesto anual entre dos fechas.  
*Valor:* Mide tendencia de ventas sostenible; ideal para presentaciones.

**% Variación MoM/YoY**
```sql
WITH agg AS (
  SELECT YEAR(fecha) AS año, MONTH(fecha) AS mes, SUM(monto) AS ventas
  FROM db_silver.tb_ventas
  GROUP BY año, mes
)
SELECT
  año, mes, ventas,
  LAG(ventas,1) OVER (ORDER BY año,mes) AS ventas_prev,
  (ventas - ventas_prev) / ventas_prev * 100 AS pct_variación
FROM agg;
```
*Descripción:* Compara ventas mes a mes o año a año.  
*Valor:* Identifica estacionalidad y evalúa campañas.

## 3. Participación y Contribución

**% Participación de Canal**
```sql
WITH totales AS (
  SELECT canal, SUM(monto) AS canal_ventas
  FROM db_silver.tb_ventas
  GROUP BY canal
), total_all AS (
  SELECT SUM(canal_ventas) AS ventas_global FROM totales
)
SELECT
  t.canal,
  t.canal_ventas,
  t.canal_ventas / a.ventas_global * 100 AS pct_participación
FROM totales t CROSS JOIN total_all a;
```
*Descripción:* Peso de cada canal sobre el total de ventas.  
*Valor:* Guiar inversión en marketing según aporte de canales.

## 4. Acumulados y Tendencias

**Running Total (Acumulado)**
```sql
SELECT
  fecha,
  SUM(monto) OVER (
    ORDER BY fecha 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS acumulado_ventas
FROM db_silver.tb_ventas;
```
*Descripción:* Suma progresiva de ventas.  
*Valor:* Monitoreo MTD/YTD vs metas.

**Promedio Móvil (3 días)**
```sql
SELECT
  fecha, monto,
  AVG(monto) OVER (
    ORDER BY fecha
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS mov_avg_3días
FROM db_silver.tb_ventas;
```
*Descripción:* Media de últimos 3 días.  
*Valor:* Suaviza fluctuaciones.

## 5. Ratios y KPIs

**Tasa de Conversión**
```sql
SELECT
  canal,
  SUM(CASE WHEN evento = 'venta' THEN 1 END) * 1.0 /
  SUM(CASE WHEN evento = 'clic' THEN 1 END) AS conv_rate
FROM db_silver.tb_eventos
GROUP BY canal;
```
*Descripción:* Proporción de clics que generan ventas.  
*Valor:* Eficiencia del embudo.

**Margen (%)**
```sql
SELECT
  producto,
  (SUM(monto) - SUM(costo)) / SUM(monto) * 100 AS margen_pct
FROM db_silver.tb_ventas
GROUP BY producto;
```
*Descripción:* Rentabilidad neta por producto.  
*Valor:* Ajuste de precios y enfoque en productos rentables.

## 6. Ranking y Top N

**TOP 10 Productos**
```sql
SELECT producto, SUM(monto) AS total_ventas
FROM db_silver.tb_ventas
GROUP BY producto
ORDER BY total_ventas DESC
LIMIT 10;
```
*Descripción:* Los 10 productos con mayores ventas.  
*Valor:* Identificar líneas prioritarias para stock/promos.

**Ranking de Canales (RANK)**
```sql
SELECT
  canal,
  SUM(monto) AS ventas,
  RANK() OVER (ORDER BY SUM(monto) DESC) AS posición
FROM db_silver.tb_ventas
GROUP BY canal;
```
*Descripción:* Posicionamiento de canales según ventas.  
*Valor:* Comparaciones claras en reportes de desempeño.
