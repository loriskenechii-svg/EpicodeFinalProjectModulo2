import pandas as pd
import dask as dd


#ESERCIZIO 1

for i in range(5):
    file = f"data_local/json/transactions_batch_{i:04d}.json"
    df = pd.read_json(file, lines=True)

    print(df.head())

#1. 
    somma = df["amount"].sum()
    totale += somma

    print("Totale generale:", totale)

#2.
df_dask = dd.read_json("data_local/json/*.json", lines=True)
print(df_dask.head())

3#
