"""Data cleaning module"""
import argparse
from pathlib import Path
import pandas as pd

OUTPUT_DIR = Path(__file__).parent / "data"


class Cleaner:  # pylint: disable=too-few-public-methods
    """Class to clean data"""

    # data specifics
    years_in_data = range(1960, 2022)
    year_column = "year"
    value_column = "value"
    region_column = "region"
    geo_time_column = "geo\\time"
    identifier_column = "unit,sex,age,geo\\time"
    identifier_column_seperator = ","
    nan_symbol = ":"

    def __init__(self, input_path: str, output_path: str, region: str) -> None:
        self.df = self._load_df(input_path)
        self.output_path = output_path
        self.region = region

    @staticmethod
    def _load_df(input_path: str) -> pd.DataFrame:
        return pd.read_csv(input_path, sep=r"[\t,]", header=0, engine="python")

    def _unpivot(self) -> None:
        self.df = pd.melt(
            self.df,
            id_vars=self.identifier_column.split(","),
            value_vars=[
                str(year)
                for year in self.years_in_data
            ],
        )

    def _drop_values_with_nan(self) -> None:
        self.df = self.df[self.df[self.value_column] != self.nan_symbol]
        self.df = self.df.dropna(subset=[self.value_column])

    def save_df(self) -> None:
        self.df.to_csv(self.output_path, index=False)

    def run(self) -> None:
        "run the data cleaner"
        self.df.columns = [col.strip() for col in self.df.columns]
        self._unpivot()
        self.df = self.df.rename(columns={self.geo_time_column: self.region_column, "variable": self.year_column})
        self.df = self.df.astype({self.year_column: "int32"})
        self.df[self.value_column] = self.df[self.value_column].str.split().str[0]
        self._drop_values_with_nan()
        self.df = self.df.astype({self.value_column: "float64"})
        self.df = self.df[self.df[self.region_column] == self.region]


def clean_data(region: str) -> None:
    "Necessary due to assignement"
    cleaner = Cleaner(
        OUTPUT_DIR / "eu_life_expectancy_raw.tsv",
        OUTPUT_DIR / f"{region.lower()}_life_expectancy.csv",
        region,
    )
    cleaner.run()
    cleaner.save_df()


if __name__ == "__main__":  # pragma: no cover
    parser = parser = argparse.ArgumentParser(description="Demo")
    parser.add_argument("--region", help="region filter", required=False, default="PT")
    region_filter = parser.parse_args().region.upper()
    print (region_filter)
    clean_data(region_filter)
