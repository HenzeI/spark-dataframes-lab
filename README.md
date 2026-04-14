# Spark DataFrames Lab con entorno dockerizado

Repositorio base de la práctica de **Apache Spark con DataFrames**. Este repositorio incluye tanto el **enunciado** como el **entorno dockerizado** necesario para ejecutar la práctica en clase.

## Qué incluye este repositorio

- `spark_jupyter/`: entorno dockerizado Spark + Jupyter proporcionado para clase
- `spark_jupyter/docker-compose-jupyter.yml`: despliegue de Spark master, workers, history server y notebook Jupyter
- `spark_jupyter/notebooks/iniciar_spark.py`: utilidad para crear la `SparkSession`
- `spark_jupyter/notebooks/practica_clientes_pedidos.ipynb`: cuaderno que debes completar
- `spark_jupyter/apps/datos/clientes.csv`: datos de clientes
- `spark_jupyter/apps/datos/pedidos.csv`: datos de pedidos
- `docs/enunciado.md`: enunciado detallado
- `docs/evidencias.md`: plantilla de evidencias
- `docs/pistas.md`: pistas técnicas

## Flujo de trabajo obligatorio

1. Haz **Fork** del repositorio del profesor.
2. Clona tu fork en local.
3. Levanta el entorno dockerizado.
4. Abre Jupyter y trabaja sobre el notebook.
5. Haz commits progresivos.
6. Sube tus cambios a GitHub.
7. Crea el tag final `v1.0-entrega`.

## Cómo levantar el entorno dockerizado

Sitúate dentro de la carpeta `spark_jupyter` y ejecuta:

```bash
docker compose -f docker-compose-jupyter.yml up -d --build
```

## Servicios disponibles

Una vez levantado el entorno, podrás acceder a:

- **JupyterLab**: http://localhost:8888  
  Token: `spark`
- **Spark Master UI**: http://localhost:8080
- **Spark Worker 1 UI**: http://localhost:8081
- **Spark Worker 2 UI**: http://localhost:8082
- **Spark History Server**: http://localhost:18080

## Qué notebook debes completar

Trabaja sobre:

`spark_jupyter/notebooks/practica_clientes_pedidos.ipynb`

## Dónde están los datos

Dentro del notebook, los datos se encuentran montados en el contenedor en:

- `/opt/spark-apps/datos/clientes.csv`
- `/opt/spark-apps/datos/pedidos.csv`

Los resultados pueden guardarse, por ejemplo, en:

- `/opt/spark-apps/salida/`

## Entrega

Debes entregar:

- el enlace a tu repositorio (fork)
- el notebook completado
- el tag obligatorio `v1.0-entrega`

```bash
git tag v1.0-entrega
git push origin v1.0-entrega
```
