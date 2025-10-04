#  Crypto Lakehouse — Bitcoin (Medallion Architecture)
---
### Overview
Este projeto constrói um **pipeline de dados** (Bronze → Silver → Gold → Insights) no **Databricks Free Edition**, usando um dataset histórico de Bitcoin (`coin_Bitcoin.csv`) baixado do Kaggle.  
O objetivo é transformar dados crus de séries temporais (preços/volume/capitalização) em **datasets analíticos prontos para consumo**, aplicando boas práticas de engenharia de dados: ingestão, limpeza, validação, enriquecimento, agregação e visualização.

---

## Por que resolver esse problema dessa forma?
O problema tratado aqui é **como transformar dados crus de séries temporais em dados confiáveis e prontos para análise**. Este problema é recorrente em muitos domínios (finanças, IoT, logística, telemetria, sensores etc.). Não é sobre Bitcoin em si, o Bitcoin foi só a fonte dos dados de exemplo; o foco é a **capacidade de processar, validar e entregar dados analíticos de qualidade**.

**Importância de lidar com esse tipo de problema:**
- **Decisões confiáveis**: relatórios e modelos só são tão bons quanto os dados que os alimentam. Limpeza e validação evitam decisões erradas.
- **Reprodutibilidade**: salvar artefatos (Parquet) por camada permite reproduzir análises e auditar alterações.
- **Escalabilidade e manutenção**: separar responsabilidades por camadas facilita evolução do pipeline (correções, novas fontes, retroprocessamento).
- **Performance**: formatos column-store (Parquet) e partições aumentam desempenho de leitura e redução de custos computacionais.

**Solução adotada em resumo:** arquitetura Medallion + persistência em Parquet + combinação Pandas (transformações) + Spark SQL (consultas analíticas). Isso entrega flexibilidade para experimentação e padrões que se aproximam de ambientes de produção.

---

## Arquitetura do Pipeline — Medallion

**CSV (coin_Bitcoin.csv)**
    
- **Bronze(ingestão) ─── Raw Parquet (../bronze/coin_bitcoin_bronze.parquet)** 
  
- **Silver(limpeza, validação e enriquecimento) ─── Clean Parquet (../silver/coin_bitcoin_silver.parquet)**

- **Gold(calcular métricas de negócio) ─── Business Parquet (../gold/coin_bitcoin_gold.parquet)**

- **Insights(consumo/visualização) ─── relatórios / gráficos / parquet (../insights/coin_bitcoin_insights.parquet)**



### Porque usar a Arquitetura Medallion:
- **Separação de responsabilidades**: cada camada tem um propósito (raw → trusted → business-ready).
- **Facilidade de correção/retroprocessamento**: corrigir um problema na Silver atualiza automaticamente o Gold se reprocessado.
- **Governança e auditoria**: mantém trilha de transformação (quem fez o quê e quando).
- **Reaproveitamento**: múltiplos consumidores (dashboards, ML, estudos exploratórios) consomem a camada apropriada.

---

## Tech Stack:
- **Plataforma:** Databricks (Free Edition)  
- **Linguagem:** Python 3.x (Pandas) + PySpark (SparkSession para SQL)  
- **Bibliotecas:** `pandas`, `matplotlib` (visualização), `pyspark` (SQL/Views)  
- **Formato de armazenamento:** Parquet  
- **Versionamento:** Git / GitHub  
- **Observação:** caminhos relativos (`../bronze/`, `../silver/`, `../gold/`, `../insights/`)

---

## Estrutura do Repositório:
```bash
crypto_lakehouse/
├─ bronze/
│  ├─ bronze_notebook.py            # Ingestão CSV -> Parquet (cru)
│  └─ coin_bitcoin_bronze.parquet
├─ silver/
│  ├─ silver_notebook.py            # Limpeza/validação/enriquecimento
│  └─ coin_bitcoin_silver.parquet
├─ gold/
│  ├─ gold_notebook.py              # Métricas de negócio e view SQL
│  └─ coin_bitcoin_gold.parquet
├─ insights/
│  ├─ insights_notebook.py          # Consultas SQL + visualizações
│  └─ coin_bitcoin_insights.parquet
└─ README.md

```

### 1) Bronze — ingestão (raw):

- **Objetivo: ler o CSV original e persistir exatamente como veio (formato Parquet).**
- **Por que Parquet: formato columnar, eficiente para leitura parcial e analítica.**

```
# bronze_notebook.py
import pandas as pd

caminho_DataSet = "/Volumes/workspace/default/arquivos-projetos/coin_Bitcoin.csv"
df_bronze = pd.read_csv(caminho_DataSet)
df_bronze.to_parquet("../bronze/coin_bitcoin_bronze.parquet", index=False)
```
- **Resultado esperado: ../bronze/coin_bitcoin_bronze.parquet**

---

### 2) Silver — limpeza, padronização e validações:

- **Objetivo: transformar raw → trusted. Nessa camada:**
  * padronizamos nomes (snake_case, português),
  * convertimos datas (data),
  * removemos linhas vazias / duplicadas,
  * tratamos nulos (forward-fill),
  * garantimos valores numéricos não-negativos,
  * ordenamos cronologicamente,
  * normalizamos simbolo/nome.


```
# silver_notebook.py
import pandas as pd

df = pd.read_parquet("../bronze/coin_bitcoin_bronze.parquet")

# padroniza coluna names
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace("-", "_")

# converte colunas de data
for col in df:
    if "date" in col or "time" in col:
        df[col] = pd.to_datetime(df[col], errors="coerce")

df = df.dropna(how="all")
df = df.fillna(method="ffill")

# renomeações relevantes (ex.: open -> preco_abertura)
df = df.rename(columns={
    "date": "data", "sno": "ano", "name": "nome", "symbol": "simbolo",
    "high": "preco_alto", "low": "preco_baixo", "open": "preco_abertura",
    "close": "preco_fechamento", "marketcap": "capitalizacao"
})

df["data"] = df["data"].dt.strftime('%Y-%m-%d')
df = df.drop_duplicates()

# validação de números não-negativos
numeric_columns = ["preco_alto", "preco_baixo", "preco_abertura", "preco_fechamento", "capitalizacao"]
for col in numeric_columns:
    df = df[df[col] >= 0]

df = df.sort_values(by="data", ascending=True)
df["simbolo"] = df["simbolo"].str.upper()
df["nome"] = df["nome"].str.title()

df.to_parquet("../silver/coin_bitcoin_silver.parquet", index=False)

```

- **Boas práticas aplicadas no Silver:**
  * nomes consistentes (snake_case),
  * validação de integridade (valores numéricos, duplicatas),
  * ordenação temporal (importante para derivados como pct_change),
  * armazenamento de uma versão limpa e auditável.

---

### 3) Gold — métricas de negócio e view SQL (híbrido Python + Spark SQL)
- **Objetivo: gerar colunas e agregações que entregam valor de negócio:**
  * retorno_pct (variação percentual),
  * volatilidade (high - low),
  * media_movel_7d (média móvel de fechamento),
  * criação de gold Parquet e TempView para consultas SQL.

```
# gold_notebook.py (resumo)
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = pd.read_parquet("../silver/coin_bitcoin_silver.parquet")

df["retorno_pct"] = df["preco_fechamento"].pct_change() * 100
df["volatilidade"] = df["preco_alto"] - df["preco_baixo"]
df["media_movel_7d"] = df["preco_fechamento"].rolling(window=7).mean()

# salvar gold
df.to_parquet("../gold/coin_bitcoin_gold.parquet", index=False)

# criar view SQL (Spark) para análises
spark_df = spark.createDataFrame(df)
spark_df.createOrReplaceTempView("gold_bitcoin")

# exemplos de consultas SQL usadas:
#  - estatísticas por símbolo
#  - média mensal de preço, retorno e volatilidade
#  - top dias por volatilidade

```

- **Por que essa separação (Python para cálculos + SQL para agregações)?**
  * Pandas/NumPy facilitam cálculos de janela (rolling) e prototipagem rápida.
  * Spark SQL permite consultas escaláveis, criação de views e integração com componentes analíticos do Databricks.

---

### 4) Insights — consultas + visualizações e persistência de resultado:

- **Objetivo: consumir o Gold e apresentar visualizações e tabelas que ajudem a contar a história dos dados.**
- **Consultas SQL (ex.: evolução de preço, retorno médio mensal, volatilidade média mensal, top quedas/altas).**
- **Visualizações com matplotlib (séries temporais) e display() do Databricks (para gráficos rápidos).**


```
# insights_notebook.py (resumo)
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()
df_insights = pd.read_parquet("../gold/coin_bitcoin_gold.parquet")
spark.createDataFrame(df_insights).createOrReplaceTempView("insights_bitcoin")

# ex: evolução do preço
display(spark.sql("SELECT CAST(data AS DATE) as data, preco_fechamento FROM insights_bitcoin ORDER BY data"))

# ex: plot em matplotlib (Pandas)
plt.plot(df_insights["data"], df_insights["preco_fechamento"])
plt.title("Evolução do Preço do Bitcoin")
plt.show()

# opcional: salvar outputs de insights para consumo (../insights/coin_bitcoin_insights.parquet)
df_insights.to_parquet("../insights/coin_bitcoin_insights.parquet", index=False)

```

---


### Como executar (passo-a-passo):

- **Preparar o repositório no GitHub e clonar / conectar via Repos ao Databricks.**
- **Fazer upload do arquivo coin_Bitcoin.csv para o caminho utilizado:**

```
/Volumes/workspace/default/arquivos-projetos/coin_Bitcoin.csv

```

- **Abrir os notebooks na ordem e executar:**
  * ```bronze_notebook.py``` → gera ```../bronze/coin_bitcoin_bronze.parquet```
  * ```silver_notebook.py``` → gera ```../silver/coin_bitcoin_silver.parquet```
  * ```gold_notebook.py``` → gera ```../gold/coin_bitcoin_gold.parquet``` e cria view gold_bitcoin
  * ```insights_notebook.py``` → consome ```../gold/coin_bitcoin_gold.parquet```, gera displays/plots e salva insights opcionais

  - **Verificar os artefatos Parquet em cada pasta.**


---

  ### Data Dictionary (colunas principais criadas):
  - **```data``` — data do registro (YYYY-MM-DD)**
  - **```nome``` — nome da moeda (Title Case)**
  - **```simbolo``` — símbolo da moeda (UPPERCASE)**
  - **```preco_alto``` — preço máximo no período**
  - **```preco_baixo``` — preço mínimo no período**
  - **```preco_abertura``` — preço de abertura**
  - **```preco_fechamento``` — preço de fechamento**
  - **```capitalizacao``` — capitalização de mercado**
  - **```retorno_pct``` — variação percentual diária do fechamento**
  - **```volatilidade``` — preco_alto - preco_baixo**
  - **```media_movel_7d``` — média móvel de 7 períodos (fechamento)**

---

### Boas práticas aplicadas:
- **Persistir cada etapa (Parquet por camada) — facilita debugging e reprocessamento seletivo.**
- **Nomes padronizados (snake_case) — evita ambiguidade e facilita consumo por BI/SQL.**
- **Validações (valores não-negativos, duplicatas) — previne que análises e modelos sejam afetados por outliers/erros triviais.**
- **Ordenação temporal antes de cálculos de janela — essencial para consistência de ```pct_change``` e ```rolling```.**
- **Separação de responsabilidades (Medallion) — facilita quem vai manter/consumir o pipeline.**
- **Salvar outputs de insights — permite integração com ferramentas que não acessam o notebook (Power BI, Streamlit, etc.).**

---

### Conclusão — benefícios da solução aplicada:

- **Qualidade de dados: menos ruído, menos inconsistências e maior confiança para análises e modelos.**
- **Reprodutibilidade: etapas claras e artefatos persistidos (Parquet) que possibilitam auditoria e reaplicação.**
- **Escalabilidade: estrutura que facilita evolução (novas moedas, mais granularidade, integração com streaming).**
- **Clareza de consumo: Gold pronto para negócio, auxiliando áreas de analytics/BI sem reprocessar transformações técnicas.**

---

### Referências & Créditos:
- **Dataset: coin_Bitcoin.csv (Kaggle) — usado apenas como exemplo de séries temporais financeiras.**
- **Projeto inspirado na arquitetura Medallion (Bronze/Silver/Gold) e práticas de Data Lakehouse.**
