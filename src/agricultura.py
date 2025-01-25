import time
from extractor import Extractor

# https://apisidra.ibge.gov.br/home
broze_folder = "C:/Users/vinic/OneDrive/Documentos/python/project_hub/data/test"

extractor_agricultura = Extractor("5457", "8331,216,214,112,215", "2023")


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
    completed_products = []
    start_time_total = time.time()

    for produto in produtos[:]:
        start_time = time.time()
        data = extractor_agricultura.get_data(product=produto)
        data.to_parquet(f"{broze_folder}/{produto}.parquet")
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


if __name__ == __name__:
    data = get_all()
    data
