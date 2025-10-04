# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

# Inicializa Spark (necessário para usar SQL no Databricks)
spark = SparkSession.builder.getOrCreate()

caminho_silver = "../silver/coin_bitcoin_silver.parquet"
df_gold = pd.read_parquet(caminho_silver)
display(df_gold)
