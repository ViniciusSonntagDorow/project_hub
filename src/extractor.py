import sidrapy
import pandas as pd


class Extractor:
    def __init__(
        self, table: str, variable: str, period: str, classification: str
    ) -> None:
        self.table_code = table
        self.territorial_level = "6"
        self.ibge_territorial_code = "all"
        self.variable = variable
        self.period = period
        self.classification = classification

    def get_data(self, product: list[str]) -> pd.DataFrame:
        try:
            data = sidrapy.get_table(
                table_code=self.table_code,
                territorial_level=self.territorial_level,
                ibge_territorial_code=self.ibge_territorial_code,
                variable=self.variable,
                classifications={self.classification: product},
                period=self.period,
            )
        except Exception as e:
            print(f"Error fetching data for product {product}: {e}")
            return pd.DataFrame()
        return data

    def get_data_small(self) -> pd.DataFrame:
        try:
            data = sidrapy.get_table(
                table_code=self.table_code,
                territorial_level=self.territorial_level,
                ibge_territorial_code=self.ibge_territorial_code,
                variable=self.variable,
                period=self.period,
            )
        except Exception as e:
            print(f"Error fetching data: {e}")
            return pd.DataFrame()
        return data
