import requests
import time
import json
import datetime

def get_data(ano:str) -> dict:
    url = f"https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata/CusteioInvestimentoComercialIndustrialSemFiltros?$filter=AnoEmissao%20eq%20'{ano}'&$format=json"
    r = requests.get(url)
    return r.json()

def save_data(data, ano:str) -> None:
    #today = datetime.datetime.strftime("%Y%m%d_%H%M%S.%f")
    with open(f"./data/{ano}.json", "w") as f:
        json.dump(data, f)

def get_save(*anos:str) -> None:
    for ano in anos:
        data = get_data(ano)
        save_data(data, ano)
        time.sleep(60)

get_save("2017", "2018")