import sidrapy
import pandas as pd
from typing import List


class Extractor:
    def __init__(self, table: str, variable: str, period: str) -> None:
        self.table_code = table
        self.territorial_level = "6"
        self.ibge_territorial_code = "all"
        self.variable = variable
        self.period = period

    def get_data(self, product: List[str]) -> pd.DataFrame:
        try:
            data = sidrapy.get_table(
                table_code=self.table_code,
                territorial_level=self.territorial_level,
                ibge_territorial_code=self.ibge_territorial_code,
                variable=self.variable,
                classifications={"782": product},
                period=self.period,
            )
        except Exception as e:
            print(f"Error fetching data for product {product}: {e}")
            return pd.DataFrame()
        return data
