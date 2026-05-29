"""
05_queries.py  —  Consultas de Verificación sobre la Capa Gold
Lee cada tabla Delta de la capa Gold y muestra sus primeros 20 registros.
Útil para validar resultados y generar evidencias del pipeline.
"""

from jobs.spark_session import crear_sesion

TABLAS_GOLD = [
    ("delta/gold/kpis_globales", "KPIs Globales del Negocio"),
    ("delta/gold/ventas_por_categoria", "Ventas por Categoría de Producto"),
    ("delta/gold/ventas_por_pais", "Ventas por País Centroamericano"),
    ("delta/gold/ventas_por_trimestre", "Tendencia Trimestral de Ventas"),
    ("delta/gold/top10_productos", "Top 10 Productos por Ingreso"),
    ("delta/gold/ventas_por_canal", "Participación por Canal de Venta"),
]


def main() -> None:
    spark = crear_sesion("FreshMart — Consultas Gold")

    for ruta, descripcion in TABLAS_GOLD:
        separador = "=" * 70
        print(f"\n{separador}")
        print(f"  {descripcion}")
        print(f"  Ruta: {ruta}")
        print(separador)
        spark.read.format("delta").load(ruta).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
