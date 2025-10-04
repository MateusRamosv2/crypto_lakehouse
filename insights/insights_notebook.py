# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

caminho_gold = "../gold/coin_bitcoin_gold.parquet"
df_gold = spark.read.parquet(caminho_gold)
