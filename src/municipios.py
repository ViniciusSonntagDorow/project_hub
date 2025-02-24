import requests
import pandas as pd
import numpy as np


def get_data():
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    response = requests.get(url)
    df = pd.DataFrame(response.json())
    df = pd.json_normalize(df.to_dict(orient="records"))
    return df


def transform_data(df):
    df = (
        df.rename(
            columns={
                "id": "cod_municipio",
                "nome": "nome_municipio",
                "microrregiao.id": "cod_microrregiao",
                "microrregiao.nome": "nome_microrregiao",
                "microrregiao.mesorregiao.id": "cod_mesorregiao",
                "microrregiao.mesorregiao.nome": "nome_mesorregiao",
                "microrregiao.mesorregiao.UF.id": "cod_uf",
                "microrregiao.mesorregiao.UF.sigla": "sigla_uf",
                "microrregiao.mesorregiao.UF.nome": "nome_uf",
                "microrregiao.mesorregiao.UF.regiao.id": "cod_regiao",
                "microrregiao.mesorregiao.UF.regiao.nome": "nome_regiao",
                "microrregiao.mesorregiao.UF.regiao.sigla": "sigla_regiao",
            }
        )
        .drop(
            columns=[
                "regiao-imediata.id",
                "regiao-imediata.nome",
                "regiao-imediata.regiao-intermediaria.id",
                "regiao-imediata.regiao-intermediaria.nome",
                "regiao-imediata.regiao-intermediaria.UF.id",
                "regiao-imediata.regiao-intermediaria.UF.sigla",
                "regiao-imediata.regiao-intermediaria.UF.nome",
                "regiao-imediata.regiao-intermediaria.UF.regiao.id",
                "regiao-imediata.regiao-intermediaria.UF.regiao.sigla",
                "regiao-imediata.regiao-intermediaria.UF.regiao.nome",
            ]
        )
        .astype(
            {
                "cod_municipio": np.int32,
                "cod_microrregiao": np.int32,
                "cod_mesorregiao": np.int16,
                "cod_uf": np.int8,
                "cod_regiao": np.int8,
                "nome_municipio": "category",
                "nome_microrregiao": "category",
                "nome_mesorregiao": "category",
                "sigla_uf": "category",
                "nome_uf": "category",
                "nome_regiao": "category",
                "sigla_regiao": "category",
            }
        )
    )
    return df


if __name__ == "__main__":
    df = transform_data(get_data())
    df.to_parquet("data/silver/municipios_silver.parquet", index=False)
