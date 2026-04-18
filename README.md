# Práctica Spark DataFrames — Clientes y Pedidos

Práctica de análisis de datos con **Apache Spark** sobre un dataset de clientes y pedidos, ejecutada en un entorno **Jupyter + Docker**.

---

## Entorno

| Componente | Detalle |
|---|---|
| Apache Spark | 4.1.1 |
| Entorno | Docker (Jupyter + Spark standalone) |
| Notebook | `notebooks/practica_clientes_pedidos.ipynb` |
| Datos de entrada | `/opt/spark-apps/datos/clientes.csv` · `pedidos.csv` |
| Datos de salida | `/opt/spark-apps/salida/resultado_parquet/` |

Para levantar el entorno:

```bash
docker compose -f docker-compose-jupyter.yml up
```

---

## Datos de entrada

### clientes.csv
- **40 clientes únicos** (el fichero incluye 3 filas duplicadas, 43 en total)
- Columnas: `id_cliente`, `nombre`, `ciudad`, `segmento`
- Segmentos posibles: `Premium` / `Estandar`
- Problema conocido: espacios extra en la columna `ciudad`

### pedidos.csv
- **120 pedidos**
- Columnas: `id_pedido`, `id_cliente`, `fecha`, `producto`, `cantidad`, `precio_unitario`
- Problemas conocidos: 3 filas con `cantidad` nula, 2 filas con `producto` nulo, 10 pedidos referenciando clientes inexistentes (id 41, 42, 45, 46, 47, 48)

---

## Pasos realizados

### 1. Lectura de datos
Carga de ambos CSV con separador `;` usando `spark.read.option("sep", ";").csv(...)`.  
Se imprime el esquema (`printSchema`) y una muestra de filas para cada DataFrame.

### 2. Limpieza de datos (clientes)
- **Trim** en columnas `ciudad`, `nombre` y `segmento` para eliminar espacios extra.
- **`dropDuplicates()`** — reduce el DataFrame de 43 a 40 filas eliminando los 3 duplicados.
- **Validación de tipos**: se comprueba que `id_cliente` sea un entero válido detectando valores que no se convierten correctamente tras el `.cast("integer")`. También se verifica que `segmento` solo contenga los valores permitidos (`Premium`, `Estandar`).
- **Diagnóstico de nulos**: conteo de nulos por columna con `F.when(isNull)`.
- **Tratamiento de nulos**: `dropna` en `id_cliente` (clave primaria) y `fillna` con valores por defecto en el resto de columnas de texto.

### 3. Transformación (pedidos)
- Cast de tipos: `id_pedido` e `id_cliente` → `integer`, `fecha` → `date`, `cantidad` y `precio_unitario` → `double`.
- Nueva columna **`importe`** = `cantidad × precio_unitario` (redondeado a 2 decimales).
- Se reportan los pedidos con `cantidad`, `producto` e `importe` nulos.

### 4. Integración de datos (join)
- **Inner join** entre `clientes_clean` y `pedidos_typed` por `id_cliente`.
- Resultado: **110 de 120 pedidos** conservados.
- **10 pedidos perdidos** porque sus `id_cliente` (41, 42, 45, 46, 47, 48) no existen en el CSV de clientes. Se identifican con un `left_anti` join.

### 5. Filtrado — ventas Premium con importe ≥ 100
- Se obtienen **34 pedidos** que cumplen `segmento == "Premium"` AND `importe >= 100` (~31% del join).
- Los importes más altos corresponden a pedidos de **Portátiles** e **Impresoras**.
- Ciudades con mayor volumen económico Premium: **Alicante, Granada y Bilbao**.

### 6. Clasificación con `when`
Nueva columna `categoria_importe` según el importe:

| Categoría | Criterio |
|---|---|
| Alto | importe ≥ 500 |
| Medio | 100 ≤ importe < 500 |
| Bajo | importe < 100 |
| Sin importe | importe nulo |

Distribución resultante: **32 Alto · 51 Medio · 24 Bajo · 3 Sin importe**.

### 7. Agregaciones por ciudad y segmento
`groupBy("ciudad", "segmento")` calculando:
- `num_pedidos` — número de pedidos
- `ingresos_totales` — suma de importe
- `importe_medio` — media de importe

Combinaciones encontradas: **18**. Las ciudades con mayor ingreso total son **Bilbao Estándar** (11.685€) y **Granada Premium** (9.161€).

### 8. SQL
- Vista temporal `pedidos_clientes` creada con `createOrReplaceTempView`.
- Consulta SQL: top 15 combinaciones ciudad/segmento/producto por ingresos totales, incluyendo número de pedidos, media e importe máximo.

### 9. Muestreo y partición
- **`sample(fraction=0.2, seed=42)`** → ~21 filas (aprox. 20% del dataset).
- **`randomSplit([0.8, 0.2], seed=42)`** → 92 filas train / 18 filas test (83.6% / 16.4%).

### 10. Persistencia en Parquet
- El DataFrame de agregaciones (`agg_df`) se guarda en `/opt/spark-apps/salida/resultado_parquet` con `mode("overwrite")`.
- Se vuelve a leer con `spark.read.parquet(...)` verificando esquema y contenido.

---

## Conclusiones

- El **inner join** es eficiente para cruzar datasets en Spark, pero hay que analizar siempre los registros que se pierden con un `left_anti` join para detectar datos huérfanos o errores en los CSV.
- **`sample`** sirve para exploración rápida con un porcentaje del dataset; **`randomSplit`** divide en subconjuntos disjuntos y es el método estándar para separar train/test.
- **Parquet** preserva los tipos de datos del esquema Spark y permite leer el resultado en cualquier momento sin necesidad de volver a procesar los CSV originales.
