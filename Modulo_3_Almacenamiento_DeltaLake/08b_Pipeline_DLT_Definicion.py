# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Pipeline Declarativo — Bronze -> Silver (eventos_carrito)
# MAGIC
# MAGIC Este archivo NO se ejecuta con %run. Se registra como codigo fuente de un recurso
# MAGIC **Lakeflow Declarative Pipeline** (Workflows > Lakeflow Declarative Pipelines > Create Pipeline).
# MAGIC Disponible en Databricks Free Edition con el limite de 1 pipeline activo por tipo —
# MAGIC este mismo pipeline se EXTIENDE (no se duplica) hasta Gold en el Modulo 5 / Sesion 12.
# MAGIC
# MAGIC Sintaxis: `from pyspark import pipelines as dp` es la forma recomendada actual
# MAGIC (el modulo histórico `dlt` con `@dlt.table` sigue funcionando como alias, pero
# MAGIC ya no es la forma recomendada para código nuevo).

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

try:
    CATALOG_NAME = spark.conf.get("pipelines.catalog")
except Exception:
    CATALOG_NAME = "ecommerce_training"
LANDING_PATH = f"/Volumes/{CATALOG_NAME}/bronze/landing/eventos_carrito"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capa Bronze — ingesta declarativa con Auto Loader
# MAGIC
# MAGIC `@dp.table` declara QUE tabla existe. Lakeflow decide COMO y CUANDO ejecutarla
# MAGIC segun el grafo de dependencias que se infiere de las funciones `dp.read()` usadas mas abajo.

# COMMAND ----------

def eventos_carrito_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{LANDING_PATH}/_schema")
        .load(LANDING_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
    )

# Registrar como tabla del pipeline (exito en pipeline; se omite en ejecucion interactiva)
try:
    eventos_carrito_bronze = dp.table(
        name="eventos_carrito_bronze",
        comment="Ingesta incremental de eventos de carrito via Auto Loader (Lakeflow Declarative Pipelines)",
    )(eventos_carrito_bronze)
except Exception:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Capa Silver — calidad de datos declarativa con expectativas
# MAGIC
# MAGIC `@dp.expect_or_drop` es el equivalente declarativo a los filtros de calidad que se
# MAGIC escribieron a mano en el Modulo 2 (Tema 2.2) — la diferencia es que Lakeflow reporta
# MAGIC automaticamente cuantos registros violaron cada regla, sin codigo adicional.

# COMMAND ----------

def eventos_carrito_silver():
    return dp.read_stream("eventos_carrito_bronze").dropDuplicates(["evento_id"])

# Registrar como tabla del pipeline con expectativas (exito en pipeline; se omite en ejecucion interactiva)
try:
    eventos_carrito_silver = dp.expect("tipo_evento_conocido", "tipo_evento IN ('view_product','add_to_cart','remove_from_cart','begin_checkout','purchase')")(eventos_carrito_silver)
    eventos_carrito_silver = dp.expect_or_drop("precio_valido", "precio_unitario > 0")(eventos_carrito_silver)
    eventos_carrito_silver = dp.expect_or_drop("cantidad_valida", "cantidad > 0")(eventos_carrito_silver)
    eventos_carrito_silver = dp.table(
        name="eventos_carrito_silver",
        comment="Eventos de carrito limpios y validados — capa Silver declarativa",
    )(eventos_carrito_silver)
except Exception:
    pass