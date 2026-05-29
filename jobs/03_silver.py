"""
03_silver.py  —  Capa Silver: Limpieza y Normalización
Responsabilidades:
  - Eliminar registros duplicados por id_transaccion
  - Corregir tipos de datos (fecha, numéricos)
  - Estandarizar texto (trim, capitalización)
  - Calcular campo derivado: total_gtq = unidades * precio_unitario_gtq
  - Extraer dimensiones temporales: anio y trimestre
  - Filtrar filas inválidas (fechas nulas, precios o unidades en cero)
"""

from pyspark.sql import functions as F
from jobs.spark_session import crear_sesion

RUTA_BRONZE = "delta/bronze/ventas_raw"
RUTA_SILVER = "delta/silver/ventas_limpias"


def main() -> None:
    spark = crear_sesion("FreshMart — Silver Layer")

    df_bronze = spark.read.format("delta").load(RUTA_BRONZE)

    df_limpio = (
        df_bronze
        # Eliminar duplicados exactos por clave de negocio
        .dropDuplicates(["id_transaccion"])
        # Conversión y normalización de fecha
        .withColumn("fecha_compra", F.to_date("fecha_compra"))
        # Estandarización de campos de texto
        .withColumn("pais", F.initcap(F.trim(F.col("pais"))))
        .withColumn("sucursal", F.trim(F.col("sucursal")))
        .withColumn("producto", F.trim(F.col("producto")))
        .withColumn("categoria", F.initcap(F.trim(F.col("categoria"))))
        .withColumn("canal_venta", F.trim(F.col("canal_venta")))
        .withColumn("metodo_pago", F.trim(F.col("metodo_pago")))
        # Asegurar tipos numéricos correctos
        .withColumn("unidades", F.col("unidades").cast("int"))
        .withColumn("precio_unitario_gtq", F.col("precio_unitario_gtq").cast("double"))
        # Campo calculado: ingreso bruto de la línea
        .withColumn("total_gtq", F.round(F.col("unidades") * F.col("precio_unitario_gtq"), 2))
        # Dimensiones de tiempo para análisis posterior
        .withColumn("anio", F.year("fecha_compra"))
        .withColumn("trimestre", F.quarter("fecha_compra"))
        .withColumn("mes", F.month("fecha_compra"))
        # Filtros de calidad: descartar datos sin sentido de negocio
        .filter(F.col("fecha_compra").isNotNull())
        .filter(F.col("unidades") > 0)
        .filter(F.col("precio_unitario_gtq") > 0)
    )

    df_limpio.write.format("delta").mode("overwrite").save(RUTA_SILVER)

    print(f"[Silver] Tabla creada en: {RUTA_SILVER}")
    print(f"[Silver] Registros limpios: {df_limpio.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()
