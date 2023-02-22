"""Data cleaning module"""
import argparse
import pandas as pd

class Cleaner: # pylint: disable=too-few-public-methods  
    """Class to clean data"""

    # data specifics
    years_in_data = range(1960, 2022)
    year_column = "year"
    value_column = "value"
    region_column = "region"
    rename_columns = {"geo\\time": region_column, "variable": year_column}
    identifier_column = "unit,sex,age,geo\\time"
    identifier_column_seperated = ["unit", "sex", "age", "geo\\time"]
    identifier_column_seperator = ","
    nan_symbol = ":"

    def __init__(self, input_path: str, output_path: str, region: str) -> None:
        self.input_path = input_path
        self.output_path = output_path
        self.region = region

    def _load_from_input_path(self) -> pd.DataFrame:
        return pd.read_csv(self.input_path, sep="\t", header=0)

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns=self.rename_columns)

    def _seperate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.identifier_column_seperated] = df[self.identifier_column].str.split(
            self.identifier_column_seperator, expand=True
        )
        return df

    @staticmethod
    def _year2str_and_append_whitespace(year: int) -> str:
        # necessary due to data qualty
        # year will later on be converted to int
        return str(year) + " "

    def _unpivot(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.melt(
            df,
            id_vars=self.identifier_column_seperated,
            value_vars=[
                self._year2str_and_append_whitespace(year)
                for year in self.years_in_data
            ],
        )

    def _year2int(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.year_column] = df[self.year_column].str.strip()
        df[self.year_column] = df[self.year_column].astype("int32")
        return df

    def _drop_values_with_nan(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df[self.value_column] != self.nan_symbol]
        df = df.dropna(subset=[self.value_column])
        return df

    def _replace_value_with_first_word(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.value_column] = df[self.value_column].str.split().str[0]
        return df

    def _value2float(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.value_column] = df[self.value_column].astype("float64")
        return df

    def _apply_region_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df[self.region_column] == self.region]

    def _save_to_output_path(self, df: pd.DataFrame) -> None:
        df.to_csv(self.output_path, index=False)

    def run(self) -> None:
        "run the data cleaner"
        df = self._load_from_input_path()
        df = self._seperate_columns(df)
        df = self._unpivot(df)
        df = self._rename_columns(df)
        df = self._year2int(df)
        df = self._replace_value_with_first_word(df)
        df = self._drop_values_with_nan(df)
        df = self._value2float(df)
        df = self._apply_region_filter(df)
        self._save_to_output_path(df)


def clean_data(region: str) -> None:
    "Necessary due to assignement"
    cleaner = Cleaner(
        "~/code/nos-lp-foundations/assignments/life_expectancy/data/eu_life_expectancy_raw.tsv",
        f"~/code/nos-lp-foundations/assignments/life_expectancy/data/{region.lower()}_life_expectancy.csv",
        region
    )
    cleaner.run()


if __name__ == "__main__": # pragma: no cover
    parser = parser = argparse.ArgumentParser(description="Demo")
    parser.add_argument("--region",help="region filter", required=False, default="PT")
    region = parser.parse_args().region.upper()
    print (region)
    clean_data(region)
