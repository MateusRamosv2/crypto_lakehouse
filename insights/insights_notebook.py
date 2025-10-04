# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

caminho_gold = "../gold/coin_bitcoin_gold.parquet"
df_insights = pd.read_parquet(caminho_gold)

spark_df_insights = spark.createDataFrame(df_insights)

spark_df_insights.createOrReplaceTempView("insights_bitcoin")
