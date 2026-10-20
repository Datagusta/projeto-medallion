from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="gas_medallion.silver.silver_combustiveis",
    comment="Preços de combustíveis ANP tipados, limpos e validados por expectations.",
)
@dp.expect("estado_preenchido", "estado_sigla IS NOT NULL")
@dp.expect("unidade_preenchida", "unidade_medida IS NOT NULL")
@dp.expect_or_drop("preco_valido", "valor_venda > 0 AND valor_venda < 20")
@dp.expect_or_drop("data_valida", "data_coleta IS NOT NULL")
@dp.expect_or_drop("cnpj_valido", "LENGTH(cnpj_revenda) = 14")
def silver_combustiveis():
    b = spark.readStream.table("gas_medallion.bronze.bronze_combustiveis")
    return (
        b.select(
            F.upper(F.trim("regiao_sigla")).alias("regiao_sigla"),
            F.upper(F.trim("estado_sigla")).alias("estado_sigla"),
            F.upper(F.trim("municipio")).alias("municipio"),
            F.upper(F.trim("revenda")).alias("revenda"),
            F.regexp_replace(F.trim("cnpj_da_revenda"), r"[^0-9]", "").alias("cnpj_revenda"),
            F.trim("nome_da_rua").alias("nome_rua"),
            F.trim("numero_rua").alias("numero_rua"),
            F.trim("complemento").alias("complemento"),
            F.upper(F.trim("bairro")).alias("bairro"),
            F.trim("cep").alias("cep"),
            F.upper(F.trim("produto")).alias("produto"),
            F.to_date(F.trim("data_da_coleta"), "dd/MM/yyyy").alias("data_coleta"),
            F.regexp_replace(F.trim("valor_de_venda"), ",", ".").cast("decimal(10,3)").alias("valor_venda"),
            F.regexp_replace(F.trim("unidade_de_medida"), r"\s+", " ").alias("unidade_medida"),
            F.upper(F.trim("bandeira")).alias("bandeira"),
            "_source_file",
            "_ingested_at",
        )
        .dropDuplicates(["cnpj_revenda", "produto", "data_coleta"])
    )