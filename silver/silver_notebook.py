# Databricks notebook source
import pandas as pd

caminho_bronze = "../bronze/coin_bitcoin_bronze.parquet"

df_silver = pd.read_parquet(caminho_bronze)

df_silver.columns = (
    df_silver.columns.str.strip()
    .str.lower()                   
    .str.replace(" ", "_")         
    .str.replace("-", "_")         
)

for col in df_silver:
    if "date" in col or "time" in col:
        df_silver[col] = pd.to_datetime(df_silver[col], errors= "coerce")

df_silver = df_silver.dropna(how="all")
df_silver = df_silver.fillna(method="ffill")

df_silver = df_silver.rename(columns={"date": "data", "sno": "ano", "name": "nome", "symbol": "simbolo", "high": "preco_alto",
                                      "low": "preco_baixo", "open": "preco_abertura", "close": "preco_fechamento", "marketcap": "capitalizacao"})


df_silver['data'] = df_silver['data'].dt.strftime('%Y-%m-%d')
df_silver = df_silver.drop_duplicates()

numeric_columns = ["preco_alto", "preco_baixo", "preco_abertura", "preco_fechamento", "capitalizacao"]
for col in numeric_columns:
    df_silver = df_silver[df_silver[col] >= 0]
    
df_silver = df_silver.sort_values(by="data", ascending=True)

df_silver["simbolo"] = df_silver["simbolo"].str.upper()
df_silver["nome"] = df_silver["nome"].str.title()

display(df_silver)

caminho_parquet = "../silver/coin_bitcoin_silver.parquet"
df_silver.to_parquet(caminho_parquet, index=False)
