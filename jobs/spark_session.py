from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def crear_sesion(nombre_app: str = "FreshMart DataOps Lakehouse") -> SparkSession:
    """
    Crea y retorna una SparkSession configurada con soporte para Delta Lake.
    Utiliza el modo local con todos los núcleos disponibles (local[*]).
    """
    constructor = (
        SparkSession.builder
        .appName(nombre_app)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    return configure_spark_with_delta_pip(constructor).getOrCreate()
