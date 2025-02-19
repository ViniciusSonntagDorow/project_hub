import time
from extractor import Extractor
import pandas as pd
import os
import numpy as np

# https://apisidra.ibge.gov.br/home

extractor_produtos = Extractor("74", "106,215", "2022", "80")
extractor_aquicultura = Extractor("3940", "4146,215", "2022", "654")


def get_all_produtos():
    produtos = ["2682", "2685", "2686", "2687", "2683", "2684"]
    completed_products = []
    start_time_total = time.time()

    for produto in produtos[:]:
        start_time = time.time()
        data = extractor_produtos.get_data(product=produto)
        data.to_parquet(f"../data/bronze/pecuaria/producao/2022_{produto}.parquet")
        elapsed_time = time.time() - start_time
        print(
            f"Time taken for {produto}: {int(elapsed_time // 60)} min and {int(elapsed_time % 60)} sec"
        )
        completed_products.append(produto)
        print(
            f"{len(produtos) - len(completed_products)} products left to be downloaded"
        )

    elapsed_time_global = time.time() - start_time_total
    print(
        f"Time taken for all the products: {int(elapsed_time_global // 60)} minutes and {int(elapsed_time_global % 60)} seconds"
    )


def get_all_aquicultura():
    produtos = [
        "32861",
        "32865",
        "32866",
        "32867",
        "32868",
        "32869",
        "32870",
        "32871",
        "32872",
        "32873",
        "32874",
        "32875",
        "32876",
        "32877",
        "32878",
        "32879",
        "32880",
        "32881",
        "32886",
        "32887",
        "32888",
        "32889",
        "32890",
        "32891",
    ]
    completed_products = []
    start_time_total = time.time()

    for produto in produtos[:]:
        start_time = time.time()
        data = extractor_aquicultura.get_data(product=produto)
        data.to_parquet(f"../data/bronze/pecuaria/producao/2022_{produto}.parquet")
        elapsed_time = time.time() - start_time
        print(
            f"Time taken for {produto}: {int(elapsed_time // 60)} min and {int(elapsed_time % 60)} sec"
        )
        completed_products.append(produto)
        print(
            f"{len(produtos) - len(completed_products)} products left to be downloaded"
        )

    elapsed_time_global = time.time() - start_time_total
    print(
        f"Time taken for all the products: {int(elapsed_time_global // 60)} minutes and {int(elapsed_time_global % 60)} seconds"
    )


def union_data() -> pd.DataFrame:
    grouped_df = pd.DataFrame()
    for file in os.listdir("../data/bronze/pecuaria/producao/"):
        df = pd.read_parquet(f"../data/bronze/pecuaria/producao/{file}")
        grouped_df = pd.concat([grouped_df, df], ignore_index=True)
    return grouped_df


def transform_data(grouped_df: pd.DataFrame) -> pd.DataFrame:
    grouped_df.columns = grouped_df.iloc[0]
    grouped_df = grouped_df.loc[grouped_df["Valor"] != "Valor"]

    grouped_df = (
        grouped_df.drop(
            columns=[
                "Nível Territorial (Código)",
                "Nível Territorial",
                "Ano (Código)",
            ]
        )
        .assign(Valor=lambda x: x["Valor"].replace({"-": "0", "...": "0"}))
        .rename(
            columns={
                "Unidade de Medida (Código)": "cod_unidade_medida",
                "Unidade de Medida": "unidade_medida",
                "Valor": "valor",
                "Município (Código)": "cod_municipio",
                "Município": "municipio",
                "Ano": "ano",
                "Variável (Código)": "cod_variavel",
                "Variável": "variavel",
                "Tipo de produto de origem animal (Código)": "cod_produto",
                "Tipo de produto de origem animal": "produto",
            }
        )
        .astype(
            {
                "cod_unidade_medida": "int32",
                "valor": "int32",
                "cod_municipio": "int32",
                "ano": "int32",
                "cod_variavel": "int32",
                "cod_produto": "int32",
            }
        )
    )
    grouped_df = grouped_df.assign(
        valor=np.where(
            (grouped_df["cod_unidade_medida"] == 40)
            | (grouped_df["cod_unidade_medida"] == 25)
            | (grouped_df["cod_unidade_medida"] == 23),
            grouped_df["valor"] * 1000,
            grouped_df["valor"],
        ),
        unidade_medida=np.where(
            (grouped_df["cod_unidade_medida"] == 40)
            | (grouped_df["cod_unidade_medida"] == 25)
            | (grouped_df["cod_unidade_medida"] == 23),
            grouped_df["unidade_medida"].str.replace("Mil ", "").str.capitalize(),
            grouped_df["unidade_medida"],
        ),
    )
    return grouped_df


if __name__ == "__main__":
    # Bronze
    get_all_produtos()
    get_all_aquicultura()

    # Silver
    grouped = union_data()
    transformed = transform_data(grouped)
    transformed.to_parquet(
        "../data/silver/pecuaria_producao_silver.parquet", index=False
    )
