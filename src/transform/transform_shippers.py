import pandas as pd

from src.transform.core.base import BaseTransformer
from src.transform.core.normalization import PHONE_PATTERN_BR, format_phone_br
from src.transform.core.validation import (
    coerce_numeric,
    ensure_non_empty_strings,
    ensure_positive,
    ensure_regex_pattern,
    ensure_unique,
)


class ShippersTransformer(BaseTransformer):
    dataset_name = "shippers"
    log_name = "Shippers"
    required_columns = {"shipperid", "shippername", "phone"}
    rename_mapping = {
        "shipperid": "id",
        "shippername": "name",
        "phone": "phone",
    }

    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        coerce_numeric(dataframe, "shipperid")
        ensure_positive(dataframe, "shipperid")
        ensure_non_empty_strings(dataframe, ["shippername", "phone"])
        ensure_unique(dataframe, "shipperid")
        return dataframe

    def normalize_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["shippername"] = dataframe["shippername"].astype(str).str.strip().str.title()
        dataframe["phone"] = dataframe["phone"].astype(str).apply(format_phone_br)

        ensure_regex_pattern(
            dataframe,
            "phone",
            PHONE_PATTERN_BR,
            "Invalid phone format in 'phone' column: {values}",
        )

        transformed = dataframe.drop_duplicates(subset="shipperid", keep="first").copy()
        return transformed

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["id"] = dataframe["id"].astype("int64")
        return dataframe

