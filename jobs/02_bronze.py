"""
02_bronze.py  —  Capa Bronze: Ingesta sin transformaciones
Principio: los datos llegan tal como fueron generados (raw).
Se almacenan en formato Delta Lake para garantizar ACID e inmutabilidad.
No se aplican filtros ni limpiezas; eso es responsabilidad de la capa Silver.
"""

from jobs.spark_session import crear_sesion

RUTA_CSV_CRUDO = "data/ventas_crudas.csv"
RUTA_BRONZE = "delta/bronze/ventas_raw"


def main() -> None:
    spark = crear_sesion("FreshMart — Bronze Layer")

    # Leer CSV manteniendo todos los campos tal como vienen
    df_crudo = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(RUTA_CSV_CRUDO)
    )

    # Persistir en Delta Lake — modo overwrite para reprocesamiento limpio
    df_crudo.write.format("delta").mode("overwrite").save(RUTA_BRONZE)

    print(f"[Bronze] Tabla creada en: {RUTA_BRONZE}")
    print(f"[Bronze] Total de registros ingestados: {df_crudo.count():,}")

    spark.stop()


if __name__ == "__main__":
    main()
