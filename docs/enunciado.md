# Proyecto individual — Análisis de datos con Spark DataFrames en entorno dockerizado

## Descripción
En esta práctica realizarás un trabajo individual utilizando **Apache Spark** y la **DataFrame API en PySpark**, trabajando sobre el entorno dockerizado incluido en este repositorio.

El objetivo es desarrollar un flujo completo de trabajo que incluya:
- lectura de datos desde ficheros CSV
- limpieza y transformación de datos
- combinación de múltiples fuentes de datos
- análisis mediante agregaciones
- uso de consultas SQL sobre DataFrames
- exportación de resultados

## Contexto
Una pequeña empresa de comercio electrónico dispone de dos fuentes de datos:
- un fichero de **clientes**
- un fichero de **pedidos**

Ambos contienen información relevante para el análisis del negocio, pero presentan problemas habituales en entornos reales:
- datos duplicados
- valores nulos
- inconsistencias de formato
- registros sin correspondencia entre tablas

Tu tarea será preparar los datos y obtener información útil para la toma de decisiones.

## Entorno de trabajo incluido en el repositorio
El repositorio incorpora la carpeta `spark_jupyter/`, donde se encuentra todo lo necesario para levantar el entorno de clase:
- `docker-compose-jupyter.yml`
- `Dockerfile.jupyter`
- notebook de ejemplo
- fichero `iniciar_spark.py`
- carpetas de datos y salida

### Cómo levantar el entorno
Desde la carpeta `spark_jupyter/` ejecuta:

```bash
docker compose -f docker-compose-jupyter.yml up -d --build
```

### Servicios del entorno
- JupyterLab: `http://localhost:8888` (token: `spark`)
- Spark Master UI: `http://localhost:8080`
- Spark Worker 1 UI: `http://localhost:8081`
- Spark Worker 2 UI: `http://localhost:8082`
- Spark History Server: `http://localhost:18080`

## Repositorio base (obligatorio)
Debes trabajar sobre este repositorio base mediante **fork**.

### Flujo de trabajo
1. Haz **Fork** del repositorio del profesor.
2. Clona tu fork en local.
3. Levanta el entorno dockerizado.
4. Completa el notebook de la práctica.
5. Realiza commits progresivos.
6. Sube los cambios a tu repositorio.

## Qué debes completar
El notebook que debes resolver es:

`spark_jupyter/notebooks/practica_clientes_pedidos.ipynb`

Los datos de entrada se encuentran en:

- `/opt/spark-apps/datos/clientes.csv`
- `/opt/spark-apps/datos/pedidos.csv`

## Qué debes implementar

### 1. Lectura de datos
- Cargar los ficheros CSV de clientes y pedidos.
- Mostrar esquema y contenido.

### 2. Limpieza de datos
- Eliminar duplicados en clientes.
- Eliminar espacios en columnas de texto.
- Gestionar valores nulos.
- Validar tipos de datos.

### 3. Transformación
- Crear nuevas columnas (por ejemplo, `importe = cantidad * precio_unitario`).
- Convertir tipos si es necesario.

### 4. Integración de datos
- Realizar un `join` entre clientes y pedidos.
- Analizar qué registros se pierden y por qué.

### 5. Análisis
- Filtrar datos relevantes.
- Clasificar pedidos mediante condiciones (`when`).
- Calcular métricas:
  - número de pedidos
  - ingresos totales
  - medias

### 6. SQL
- Crear una vista temporal.
- Realizar al menos una consulta SQL significativa.

### 7. Muestreo y partición
- Aplicar `sample()`.
- Aplicar `randomSplit()`.

### 8. Persistencia
- Guardar resultados en formato **Parquet**.
- Leerlos de nuevo.

## Entregables
1. **Cuaderno Jupyter (.ipynb)** completamente ejecutado.
2. Evidencias incluidas en `docs/evidencias.md`.
3. Tag obligatorio de entrega:

```bash
git tag v1.0-entrega
git push origin v1.0-entrega
```

## Criterios de evaluación (0–10)

### A. Lectura y comprensión de datos — 1.5 pts
- Lectura correcta de CSV
- Uso de opciones (`sep`, `header`, `inferSchema`)
- Inspección con `show`, `printSchema`, `count`

### B. Limpieza de datos — 1.5 pts
- Eliminación de duplicados
- Tratamiento de nulos
- Uso de `trim` u otras funciones
- Coherencia en los datos

### C. Transformaciones — 1.5 pts
- Uso correcto de `withColumn`
- Cálculo de nuevas columnas
- Conversión de tipos (`cast`)

### D. Integración de datos (JOIN) — 2.0 pts
- Uso correcto de `join`
- Selección adecuada del tipo de join
- Interpretación de resultados

### E. Análisis y agregaciones — 2.0 pts
- Uso de `groupBy` y funciones agregadas
- Cálculo correcto de métricas
- Ordenación de resultados

### F. SQL sobre DataFrames — 0.5 pts
- Creación de vista temporal
- Consulta SQL correcta

### G. Muestreo y partición — 0.5 pts
- Uso de `sample()`
- Uso de `randomSplit()`

### H. Persistencia de datos — 0.5 pts
- Escritura en Parquet
- Lectura posterior correcta

### I. Calidad del código y presentación — 0.5 pts
- Código claro y ordenado
- Uso de nombres adecuados
- Comentarios explicativos

### TOTAL: 10,00 puntos
