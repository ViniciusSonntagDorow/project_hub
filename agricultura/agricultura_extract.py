import pandas as pd
import sidrapy
import numpy as np

#https://apisidra.ibge.gov.br/home

def get_data(variable,product,period) -> pd.DataFrame:
    data = sidrapy.get_table(
        table_code = "5457",
        territorial_level = "6",
        ibge_territorial_code = "all",
        variable = variable,
        classifications = {"782":product},
        period = period
    )
    return data

def get_all():
    df_full = pd.DataFrame()
    #
    for variavel in ("8331","216","214","215"):
        for periodo in ("2022","2021"):
            for produto in ("40129","40092"):
                data = get_data(variavel,produto,periodo)
                df_full = pd.concat([df_full,data])
                print(f"{variavel},{produto},{periodo}")
    return df_full

data = get_all()
data

def transform_data(df):
    df.columns = df.iloc[0]
    df = df.loc[df["Valor"]!="Valor"]
    
    df = ( df
        .drop(columns=["Nível Territorial (Código)","Nível Territorial","Unidade de Medida (Código)","Produto das lavouras temporárias e permanentes","Ano (Código)","Variável"])
        .assign(Valor = lambda x: x["Valor"].replace({"-":"0","...":"0"}))
        .drop(columns = ["Unidade de Medida","Município"])
        .rename(columns={"Município (Código)":"cod_municipio","Produto das lavouras temporárias e permanentes (Código)":"cod_produto","Ano":"ano","Variável (Código)":"cod_variavel","Valor":"valor"})
        .astype({"valor":int,"cod_municipio":int,"cod_produto":int,"cod_variavel":int,"ano":int})
    )
    df = df.assign(valor = np.where(df["cod_variavel"] == 215, df["valor"] * 1000, df["valor"]))
    return df

df = transform_data(data)

df