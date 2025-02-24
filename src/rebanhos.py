import time
from extractor import Extractor
import pandas as pd
import os

# https://apisidra.ibge.gov.br/home


extractor_rebanhos = Extractor("3939", "105", "2022", "79")


def get_all_rebanhos():
    produtos = [
        "2670",
        "2675",
        "2672",
        "32794",
        "32795",
        "2681",
        "2677",
        "32796",
        "32793",
        "2680",
    ]
    completed_products = []
    start_time_total = time.time()

    for produto in produtos[:]:
        start_time = time.time()
        data = extractor_rebanhos.get_data(product=produto)
        data.to_parquet(f"data/bronze/pecuaria/rebanhos/2022_{produto}.parquet")
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
    for file in os.listdir("data/bronze/pecuaria/rebanhos"):
        df = pd.read_parquet(f"data/bronze/pecuaria/rebanhos/{file}")
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
                "Tipo de rebanho (Código)": "cod_produto",
                "Tipo de rebanho": "produto",
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
    return grouped_df


if __name__ == "__main__":
    # Bronze
    # get_all_rebanhos()

    # Silver
    transformed = transform_data(union_data())
    transformed.to_parquet("data/silver/pecuaria_rebanhos_silver.parquet", index=False)
