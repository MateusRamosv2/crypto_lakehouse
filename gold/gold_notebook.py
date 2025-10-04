# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

# Inicializa Spark (necessário para usar SQL no Databricks)
spark = SparkSession.builder.getOrCreate()

caminho_silver = "../silver/coin_bitcoin_silver.parquet"
df_gold = pd.read_parquet(caminho_silver)

df_gold["retorno_pct"] = df_gold["preco_fechamento"].pct_change() * 100

df_gold["volatilidade"] = df_gold["preco_alto"] - df_gold["preco_baixo"]

df_gold["media_movel_7d"] = df_gold["preco_fechamento"].rolling(window=7).mean()

spark_df_gold = spark.createDataFrame(df_gold)

spark_df_gold.createOrReplaceTempView("gold_bitcoin")

display(df_gold)

# COMMAND ----------

display(spark.sql("""
SELECT 
    simbolo,
    COUNT(*) as qtd_registros,
    MIN(preco_fechamento) as preco_minimo,
    MAX(preco_fechamento) as preco_maximo,
    AVG(preco_fechamento) as preco_medio
FROM gold_bitcoin
GROUP BY simbolo
"""))

# Exemplo 2: Variação mensal média
display(spark.sql("""
SELECT 
    DATE_FORMAT(date_trunc('month', CAST(data AS DATE)), "yyyy-MM") as mes,
    AVG(preco_fechamento) as preco_medio,
    AVG(retorno_pct) as retorno_medio_pct,
    AVG(volatilidade) as volatilidade_media
FROM gold_bitcoin
GROUP BY date_trunc('month', CAST(data AS DATE))
ORDER BY mes
"""))

# Exemplo 3: Top 10 dias de maior volatilidade
display(spark.sql("""
SELECT 
    data,
    preco_alto,
    preco_baixo,
    volatilidade
FROM gold_bitcoin
ORDER BY volatilidade DESC
LIMIT 10
"""))



caminho_gold = "../gold/coin_bitcoin_gold.parquet"
df_gold.to_parquet(caminho_gold, index=False)
