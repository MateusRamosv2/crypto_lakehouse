# Databricks notebook source
import pandas as pd

caminho_DataSet = "/Volumes/workspace/default/arquivos-projetos/coin_Bitcoin.csv"

df_bronze = pd.read_csv(caminho_DataSet)

display(df_bronze.head)


caminho_parquet = "../bronze/coin_bitcoin_bronze.parquet"
df_bronze.to_parquet(caminho_parquet, index=False)
