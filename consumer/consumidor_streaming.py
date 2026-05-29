"""
consumer/consumidor_streaming.py
---------------------------------
Consumidor Spark Structured Streaming que lee el tópico Kafka
'ventas-tiempo-real' y calcula KPIs de negocio en ventanas deslizantes.

Cada micro-batch que llega desde Kafka representa eventos de compra
ocurridos en sucursales de FreshMart en toda Centroamérica.
Spark los procesa en tiempo real y muestra los resultados en consola.

KPIs calculados por ventana de 30 segundos (deslizante cada 10s):
  - Ingresos totales acumulados en la ventana (GTQ)
  - Número de transacciones
  - Ticket promedio por transacción
  - Ingresos por país
  - Categoría de producto más vendida

Uso desde dentro del contenedor:
    python consumer/consumidor_streaming.py
    python consumer/consumidor_streaming.py --broker fm_kafka:9092
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

# ── Esquema del evento JSON publicado por el productor ────────────────────────

ESQUEMA_EVENTO = StructType([
    StructField("id_transaccion",      IntegerType(), True),
    StructField("timestamp",           StringType(),  True),
    StructField("id_cliente",          IntegerType(), True),
    StructField("pais",                StringType(),  True),
    StructField("sucursal",            StringType(),  True),
    StructField("producto",            StringType(),  True),
    StructField("categoria",           StringType(),  True),
    StructField("unidades",            IntegerType(), True),
    StructField("precio_unitario_gtq", DoubleType(),  True),
    StructField("total_gtq",           DoubleType(),  True),
    StructField("canal_venta",         StringType(),  True),
    StructField("metodo_pago",         StringType(),  True),
])

# ── Sesión Spark con soporte Kafka ────────────────────────────────────────────

def crear_spark(broker: str) -> SparkSession:
    paquetes = (
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.kafka:kafka-clients:3.4.0"
    )
    return (
        SparkSession.builder
        .appName("FreshMart — Streaming KPIs en Tiempo Real")
        .master("local[*]")
        .config("spark.jars.packages", paquetes)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

# ── Procesamiento del stream ──────────────────────────────────────────────────

def procesar_stream(spark: SparkSession, broker: str, topico: str) -> None:
    """
    Lee el stream de Kafka, deserializa JSON, aplica transformaciones
    y escribe KPIs en consola de forma continua.
    """

    # 1. Leer stream desde Kafka
    stream_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topico)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2. Deserializar el valor JSON y parsear campos
    stream_eventos = (
        stream_raw
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
        .select(
            F.from_json(F.col("json_str"), ESQUEMA_EVENTO).alias("datos"),
            F.col("kafka_ts"),
        )
        .select("datos.*", "kafka_ts")
        # Convertir el timestamp del evento a tipo timestamp para ventanas
        .withColumn("evento_ts", F.to_timestamp("timestamp"))
    )

    # ── KPI 1: Resumen global por ventana deslizante (30s, cada 10s) ─────────
    kpi_global = (
        stream_eventos
        .withWatermark("evento_ts", "20 seconds")
        .groupBy(
            F.window("evento_ts", "30 seconds", "10 seconds").alias("ventana")
        )
        .agg(
            F.count("id_transaccion").alias("transacciones"),
            F.round(F.sum("total_gtq"), 2).alias("ingresos_gtq"),
            F.round(F.avg("total_gtq"), 2).alias("ticket_promedio_gtq"),
            F.sum("unidades").alias("unidades_vendidas"),
        )
        .select(
            F.col("ventana.start").alias("ventana_inicio"),
            F.col("ventana.end").alias("ventana_fin"),
            "transacciones",
            "ingresos_gtq",
            "ticket_promedio_gtq",
            "unidades_vendidas",
        )
        .orderBy("ventana_inicio")
    )

    # ── KPI 2: Ingresos por país en ventana de 30s ────────────────────────────
    kpi_pais = (
        stream_eventos
        .withWatermark("evento_ts", "20 seconds")
        .groupBy(
            F.window("evento_ts", "30 seconds", "10 seconds"),
            F.col("pais"),
        )
        .agg(
            F.count("id_transaccion").alias("transacciones"),
            F.round(F.sum("total_gtq"), 2).alias("ingresos_gtq"),
        )
        .select(
            F.col("window.start").alias("ventana_inicio"),
            "pais",
            "transacciones",
            "ingresos_gtq",
        )
        .orderBy("ventana_inicio", F.desc("ingresos_gtq"))
    )

    # ── KPI 3: Categoría más activa por ventana ───────────────────────────────
    kpi_categoria = (
        stream_eventos
        .withWatermark("evento_ts", "20 seconds")
        .groupBy(
            F.window("evento_ts", "30 seconds", "10 seconds"),
            F.col("categoria"),
        )
        .agg(
            F.round(F.sum("total_gtq"), 2).alias("ingresos_gtq"),
            F.count("id_transaccion").alias("transacciones"),
        )
        .select(
            F.col("window.start").alias("ventana_inicio"),
            "categoria",
            "transacciones",
            "ingresos_gtq",
        )
        .orderBy("ventana_inicio", F.desc("ingresos_gtq"))
    )

    # ── Escribir KPIs en consola (modo append + complete según el KPI) ────────
    query_global = (
        kpi_global.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 5)
        .trigger(processingTime="10 seconds")
        .queryName("KPI_Global")
        .start()
    )

    query_pais = (
        kpi_pais.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime="10 seconds")
        .queryName("KPI_Pais")
        .start()
    )

    query_categoria = (
        kpi_categoria.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .trigger(processingTime="10 seconds")
        .queryName("KPI_Categoria")
        .start()
    )

    print("[Consumidor] Stream activo. Esperando eventos de Kafka...")
    print("[Consumidor] Ctrl+C para detener.\n")

    # Mantener el proceso vivo hasta interrupción
    spark.streams.awaitAnyTermination()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Consumidor Spark Streaming — FreshMart")
    parser.add_argument("--broker", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topico", default="ventas-tiempo-real")
    args = parser.parse_args()

    spark = crear_spark(args.broker)
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Consumidor] Conectando a Kafka: {args.broker}")
    print(f"[Consumidor] Tópico suscrito: {args.topico}")

    procesar_stream(spark, args.broker, args.topico)


if __name__ == "__main__":
    main()
