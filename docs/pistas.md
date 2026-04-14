# Pistas técnicas

## Arrancar Spark en el notebook
```python
from iniciar_spark import get_spark
spark = get_spark("MiPractica")
```

## Rutas de datos
```python
from pathlib import Path
data_dir = Path("/opt/spark-apps/datos")
```

## Lectura de CSV
```python
df = (
    spark.read
    .option("sep", ";")
    .option("header", True)
    .option("inferSchema", True)
    .csv("/opt/spark-apps/datos/clientes.csv")
)
```

## Join
```python
df_join = df1.join(df2, on="id_cliente", how="inner")
```

## SQL
```python
df.createOrReplaceTempView("ventas")
spark.sql("SELECT * FROM ventas").show()
```

## Guardar en Parquet
```python
df.write.mode("overwrite").parquet("/opt/spark-apps/salida/resultado_parquet")
```
