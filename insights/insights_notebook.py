# Databricks notebook source
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

caminho_gold = "../gold/coin_bitcoin_gold.parquet"
df_insights = pd.read_parquet(caminho_gold)

spark_df_insights = spark.createDataFrame(df_insights)

spark_df_insights.createOrReplaceTempView("insights_bitcoin")

# COMMAND ----------

display(spark.sql("""
SELECT 
    CAST(data AS DATE) as data,
    preco_fechamento
FROM insights_bitcoin
ORDER BY data
"""))

display(spark.sql("""
SELECT 
    DATE_FORMAT(date_trunc('month', CAST(data AS DATE)), "yyyy-MM") as mes,
    AVG(retorno_pct) as retorno_medio_pct
FROM insights_bitcoin
GROUP BY date_trunc('month', CAST(data AS DATE))
ORDER BY mes
"""))

display(spark.sql("""
SELECT 
    DATE_FORMAT(date_trunc('month', CAST(data AS DATE)), "yyyy-MM") as mes,
    AVG(volatilidade) as volatilidade_media
FROM insights_bitcoin
GROUP BY date_trunc('month', CAST(data AS DATE))
ORDER BY mes
"""))

display(spark.sql("""
SELECT 
    data,
    retorno_pct
FROM insights_bitcoin
ORDER BY retorno_pct ASC
LIMIT 10
"""))

display(spark.sql("""
SELECT 
    data,
    retorno_pct
FROM insights_bitcoin
ORDER BY retorno_pct DESC
LIMIT 10
"""))

display(spark.sql("""
SELECT 
    DATE_FORMAT(date_trunc('quarter', CAST(data AS DATE)), "yyyy-Q") as trimestre,
    AVG(capitalizacao) as capitalizacao_media
FROM insights_bitcoin
GROUP BY date_trunc('quarter', CAST(data AS DATE))
ORDER BY trimestre
"""))

plt.figure(figsize=(12,6))
plt.plot(df_insights["data"], df_insights["preco_fechamento"], label="Preço de Fechamento", color="blue")
plt.title("Evolução do Preço do Bitcoin")
plt.xlabel("Data")
plt.ylabel("Preço (USD)")
plt.legend()
plt.grid(True)
plt.show()

caminho_insights = "../insights/coin_bitcoin_insights.parquet"
df_insights.to_parquet(caminho_insights, index=False)
