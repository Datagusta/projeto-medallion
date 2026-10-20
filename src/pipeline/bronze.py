import re
import unicodedata

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = "/Volumes/gas_medallion/landing/raw/anp/"


def normalizar(nome: str) -> str:
    sem_acento = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode()
    return re.sub(r"[^0-9a-zA-Z]+", "_", sem_acento).strip("_").lower()


@dp.table(
    name="gas_medallion.bronze.bronze_combustiveis",
    comment="Ingestão incremental dos CSVs da ANP via Auto Loader (dado cru, tudo string).",
)
def bronze_combustiveis():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("sep", ";")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(LANDING_PATH)
    )
    df = df.select([F.col(f"`{c}`").alias(normalizar(c)) for c in df.columns])
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path"))
    )