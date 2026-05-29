"""
04_gold.py  —  Capa Gold: Tablas Analíticas y KPIs de Negocio
Genera cinco tablas listas para consumo directo por analistas y dashboards:
  1. kpis_globales          — métricas maestras del negocio
  2. ventas_por_categoria   — rendimiento por categoría de producto
  3. ventas_por_pais        — desempeño geográfico en Centroamérica
  4. ventas_por_trimestre   — tendencia temporal (trimestral)
  5. top10_productos        — productos más vendidos por ingreso
  6. ventas_por_canal       — participación de cada canal de venta
Cada tabla se guarda en Delta Lake (para consultas ACID) y en CSV (evidencias).
"""

from pyspark.sql import functions as F
from jobs.spark_session import crear_sesion

RUTA_SILVER = "delta/silver/ventas_limpias"
BASE_GOLD = "delta/gold"
BASE_OUTPUT = "output"


def guardar_tabla_gold(df, nombre: str) -> None:
    """Persiste el DataFrame en Delta Lake y como CSV de salida."""
    ruta_delta = f"{BASE_GOLD}/{nombre}"
    ruta_csv = f"{BASE_OUTPUT}/{nombre}"
    df.write.format("delta").mode("overwrite").save(ruta_delta)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(ruta_csv)
    print(f"  → Gold guardada: {ruta_delta}  |  CSV: {ruta_csv}")


def main() -> None:
    spark = crear_sesion("FreshMart — Gold Layer")
    df = spark.read.format("delta").load(RUTA_SILVER)

    # 1. KPIs globales del negocio
    kpis_globales = df.agg(
        F.count("id_transaccion").alias("total_transacciones"),
        F.round(F.sum("total_gtq"), 2).alias("ingresos_totales_gtq"),
        F.round(F.avg("total_gtq"), 2).alias("ticket_promedio_gtq"),
        F.round(F.avg("unidades"), 2).alias("unidades_promedio_por_compra"),
        F.countDistinct("id_cliente").alias("clientes_unicos"),
        F.countDistinct("producto").alias("productos_en_catalogo"),
        F.countDistinct("sucursal").alias("sucursales_activas"),
    )

    # 2. Ventas por categoría de producto
    ventas_categoria = (
        df.groupBy("categoria")
        .agg(
            F.count("id_transaccion").alias("transacciones"),
            F.sum("unidades").alias("unidades_vendidas"),
            F.round(F.sum("total_gtq"), 2).alias("ingresos_gtq"),
            F.round(F.avg("total_gtq"), 2).alias("ticket_promedio_gtq"),
        )
        .orderBy(F.desc("ingresos_gtq"))
    )

    # 3. Ventas por país centroamericano
    ventas_pais = (
        df.groupBy("pais")
        .agg(
            F.count("id_transaccion").alias("transacciones"),
            F.round(F.sum("total_gtq"), 2).alias("ingresos_gtq"),
            F.countDistinct("id_cliente").alias("clientes_unicos"),
        )
        .orderBy(F.desc("ingresos_gtq"))
    )

    # 4. Tendencia trimestral
    ventas_trimestre = (
        df.groupBy("anio", "trimestre")
        .agg(
            F.count("id_transaccion").alias("transacciones"),
            F.round(F.sum("total_gtq"), 2).alias("ingresos_gtq"),
        )
        .orderBy("anio", "trimestre")
    )

    # 5. Top 10 productos por ingreso total
    top10_productos = (
        df.groupBy("producto", "categoria")
        .agg(
            F.sum("unidades").alias("unidades_vendidas"),
            F.round(F.sum("total_gtq"), 2).alias("ingresos_gtq"),
        )
        .orderBy(F.desc("ingresos_gtq"))
        .limit(10)
    )

    # 6. Participación por canal de venta
    ventas_canal = (
        df.groupBy("canal_venta")
        .agg(
            F.count("id_transaccion").alias("transacciones"),
            F.round(F.sum("total_gtq"), 2).alias("ingresos_gtq"),
            F.round(F.avg("unidades"), 2).alias("unidades_promedio"),
        )
        .orderBy(F.desc("ingresos_gtq"))
    )

    print("[Gold] Guardando tablas analíticas...")
    guardar_tabla_gold(kpis_globales, "kpis_globales")
    guardar_tabla_gold(ventas_categoria, "ventas_por_categoria")
    guardar_tabla_gold(ventas_pais, "ventas_por_pais")
    guardar_tabla_gold(ventas_trimestre, "ventas_por_trimestre")
    guardar_tabla_gold(top10_productos, "top10_productos")
    guardar_tabla_gold(ventas_canal, "ventas_por_canal")

    print("\n[Gold] KPIs Globales FreshMart:")
    kpis_globales.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
