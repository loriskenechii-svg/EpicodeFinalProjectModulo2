#ESERCIZIO 1 - INGESTION E LIMITI DI MEMORIA
#1. Lettura file

import pandas as pd
import glob

totale = 0

for file in glob.glob("data_local/json/*.jsonl"):
    df = pd.read_json(file, lines=True)
    totale += df["amount"].sum()

print(f"Fatturato totale: {totale}")

#2. Dask
import dask.dataframe as dd

df_dask = dd.read_json("data_local/json/*.jsonl", lines=True)
media = df_dask.groupby("payment_type")["amount"].mean()

print(media.compute())


#ESERCIZIO 2 - PIPELINE ETL CON PYSPARK
#1. Inizializzazione SparkSession

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MEGASHOPETL").getOrCreate()


#2. Caricamento tabelle 
#transazioni = spark.read.parquet("data_local/parquet/transactions_batch_*.parquet")
file = glob.glob("data_local/parquet/transactions_batch_*.parquet")

transazioni = spark.read.parquet(*file)
prodotti = spark.read.parquet("data_local/parquet/products.parquet")
regioni = spark.read.parquet("data_local/parquet/regions.parquet")

transazioni.show(3)
prodotti.show(3)
regioni.show(3) 

#3. Trasformazioni
df = transazioni.join(prodotti, on="product_id", how="left") 
df = df.join(regioni, on="region_id", how="left")

df_finale = df.select("transaction_id", "region_name", "category", "amount", "year")

df_finale.show(3) 


#4. Load
#df_finale.write.mode("overwrite").partitionBy("year").parquet("data_local/parquet/fatturato_per_regione.parquet")

df_finale.toPandas().to_parquet(
    "data_local/processed_sales",
    partition_cols=["year"],
    index=False
)


#ESERCIZIO 3 - DATA VISUALIZATION
#1. Calcolo fatturato e riporto valore aggrefato in Pandas
fatturato = df_finale.groupBy("category").sum("amount")
fatturato_pd = fatturato.toPandas()

print(fatturato_pd)

#2. Grafico a barre
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.bar(fatturato_pd["category"], fatturato_pd["sum(amount)"])
plt.xlabel("Categoria")
plt.ylabel("Fatturato")
plt.title("Fatturato per Categoria")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("fatturato_per_categoria.png")
plt.show()