"""Data cleaning module"""
import argparse
from pathlib import Path
import pandas as pd

DIR = Path(__file__).parent / "data"
INPUT_FILE = "eu_life_expectancy_raw.tsv"


class Cleaner:  # pylint: disable=too-few-public-methods
    """Class to clean data"""

    data_specifics = {
        "years_in_data": range(1960, 2022),
        "year_column": "year",
        "value_column": "value",
        "region_column": "region",
        "geo_time_column": "geo\\time",
        "value_column": "value",
        "identifier_column": "unit,sex,age,geo\\time",
        "identifier_column_seperator": ",",
        "nan_symbol": ":",
    }

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
            id_vars=self.data_specifics["identifier_column"].split(","),
            var_name=self.data_specifics["year_column"],
            value_name=self.data_specifics["value_column"]
            # value_vars=[str(year) for year in self.data_specifics["years_in_data"]],
        )

    def _drop_values_with_nan(self) -> None:
        self.df = self.df[self.df[self.data_specifics["value_column"]] != self.data_specifics["nan_symbol"]]
        self.df = self.df.dropna(subset=[self.data_specifics["value_column"]])

    def save_df(self) -> None:
        self.df.to_csv(self.output_path, index=False)

    def run(self) -> None:
        "run the data cleaner"
        self.df.columns = [col.strip() for col in self.df.columns]
        self._unpivot()
        self.df = self.df.rename(
            columns={
                self.data_specifics["geo_time_column"]: self.data_specifics["region_column"],
                # "variable": self.data_specifics["year_column"],
            }
        )
        self.df = self.df.astype({self.data_specifics["year_column"]: "int32"})
        self.df[self.data_specifics["value_column"]] = self.df[self.data_specifics["value_column"]].str.split().str[0]
        self._drop_values_with_nan()
        self.df = self.df.astype({self.data_specifics["value_column"]: "float64"})
        self.df = self.df[self.df[self.data_specifics["region_column"]] == self.region]

def load_clean_save_data(region: str) -> None:
    output_file = f"{region.lower()}_life_expectancy.csv"
    cleaner = Cleaner(
        DIR / INPUT_FILE,
        DIR / output_file,
        region,
    )
    cleaner.run()
    cleaner.save_df()

def main() -> None:
    parser = parser = argparse.ArgumentParser(description="Demo")
    parser.add_argument("--region", help="region filter", required=False, default="PT")
    region_filter = parser.parse_args().region.upper()
    load_clean_save_data(region_filter)


if __name__ == "__main__":  # pragma: no cover
    main()
