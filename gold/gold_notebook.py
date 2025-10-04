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

display(df_gold)
