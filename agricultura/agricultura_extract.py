import pandas as pd
import sidrapy
import numpy as np

# https://apisidra.ibge.gov.br/home


def get_data(product) -> pd.DataFrame:
    data = sidrapy.get_table(
        table_code="5457",
        territorial_level="6",
        ibge_territorial_code="all",
        variable="8331,216,214,112,215",
        classifications={"782": product},
        period="2023",
    )
    return data


def get_all():
    produtos = [
        "40129",
        "40092",
        "45982",
        "40329",
        "40130",
        "40099",
        "40100",
        "40101",
        "40102",
        "40103",
        "40131",
        "40136",
        "40104",
        "40105",
        "40137",
        "40468",
        "40138",
        "40139",
        "40140",
        "40141",
        "40330",
        "40106",
        "40331",
        "40142",
        "40143",
        "40107",
        "40108",
        "40109",
        "40144",
        "40145",
        "40146",
        "40147",
        "40110",
        "40111",
        "40112",
        "40148",
        "40113",
        "40114",
        "40149",
        "40150",
        "40115",
        "40151",
        "40152",
        "40116",
        "40260",
        "40117",
        "40261",
        "40118",
        "40119",
        "40262",
        "40263",
        "40264",
        "40120",
        "40121",
        "40122",
        "40265",
        "40266",
        "40267",
        "40268",
        "40269",
        "40123",
        "40270",
        "40124",
        "40125",
        "40271",
        "40126",
        "40127",
        "40128",
        "40272",
        "40273",
        "40274",
    ]
    df_full = pd.DataFrame()
    for produto in produtos:
        data = get_data(product=produto)
        df_full = pd.concat([df_full, data])
        print(f"{produto}")
    return df_full


data = get_all()
data
