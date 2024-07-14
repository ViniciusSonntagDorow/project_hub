import pandas as pd
import os
import json

def get_raw() -> pd.DataFrame:
    df_full = pd.DataFrame()
    for file in os.listdir("./data"):
        with open(f"./data/{file}") as f:
            data = json.load(f)
            df = pd.DataFrame.from_dict(data["value"])
            df_full = pd.concat([df, df_full])
    return df_full

df = get_raw()

def clear_df(df:pd.DataFrame) -> pd.DataFrame:
    df = ( df
        .assign(data_operacao = lambda x:x["MesEmissao"].str.cat(x["AnoEmissao"],sep="/").apply(lambda x:f"01/{x}"))
        .rename(columns={"cdPrograma":"id_programa","cdSubPrograma":"id_subprograma","cdFonteRecurso":"id_fonterecurso","Atividade":"id_atividade","codMunicIbge":"id_municipio","MesEmissao":"mes","AnoEmissao":"ano"})
        
        .drop(columns=["AreaCusteio","AreaInvestimento","Municipio","nomeUF","cdMunicipio","cdEstado","mes","ano"])
        .astype({"QtdCusteio":str,"VlCusteio":str,"QtdInvestimento":str,"VlInvestimento":str,"QtdIndustrializacao":str,"VlIndustrializacao":str,"QtdComercializacao":str,"VlComercializacao":str})
        .assign(custeio = lambda x: x["QtdCusteio"].str.cat(x["VlCusteio"],sep="/"),
                investimento = lambda x: x["QtdInvestimento"].str.cat(x["VlInvestimento"],sep="/"),
                comercializacao = lambda x: x["QtdComercializacao"].str.cat(x["VlComercializacao"],sep="/"),
                industrializacao = lambda x: x["QtdIndustrializacao"].str.cat(x["VlIndustrializacao"],sep="/"))
        .drop(columns=["QtdCusteio","VlCusteio","QtdInvestimento","VlInvestimento","QtdComercializacao","VlComercializacao","QtdIndustrializacao","VlIndustrializacao"])
        .set_index(["id_municipio","data_operacao","id_programa","id_subprograma","id_fonterecurso","id_atividade"])
        .stack()
        .reset_index()
        .rename(columns={"level_6":"id_finalidade",0:"qtd/valor"})
        .assign(quantidade = lambda x:x["qtd/valor"].str.split("/").str[0],valor = lambda x:x["qtd/valor"].str.split("/").str[1])
        .drop(columns="qtd/valor")
        .astype({"quantidade":float,"valor":float})
        .query("quantidade > 0")
        .assign(id_finalidade = lambda x: x["id_finalidade"].replace({"custeio":"1","investimento":"2","comercializacao":"3","industrializacao":"4"}))
    )
    return df

df_final = clear_df(df)