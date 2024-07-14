import pandas as pd
import os

def read_data():
    df_full = pd.DataFrame()
    for file in os.listdir("./data"):
        df = pd.read_csv(file, sep=";")
        df_full = pd.concat([df,df_full])
    return df_full

read_data()