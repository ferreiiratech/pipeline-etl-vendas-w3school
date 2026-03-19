import logging

import pandas as pd

from src.transform.core.base import BaseTransformer
from src.transform.core.validation import (
    coerce_numeric,
    ensure_non_empty_strings,
    ensure_positive,
    ensure_unique,
)


class CategoryTransformer(BaseTransformer):
    dataset_name = "categories"
    log_name = "Categories"
    required_columns = {"categoryid", "categoryname"}
    rename_mapping = {
        "categoryid": "id",
        "categoryname": "name",
        "description": "description",
    }

    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        coerce_numeric(dataframe, "categoryid")
        ensure_positive(dataframe, "categoryid")
        ensure_unique(dataframe, "categoryid")
        ensure_non_empty_strings(dataframe, ["categoryname"])

        if "description" not in dataframe.columns:
            dataframe["description"] = ""
            logging.warning("Column 'description' not found in categories; defaulting to empty values.")

        return dataframe

    def normalize_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["categoryname"] = dataframe["categoryname"].astype(str).str.strip().str.title()
        dataframe["description"] = dataframe["description"].fillna("").astype(str).str.strip()
        return dataframe

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["id"] = dataframe["id"].astype("int64")
        return dataframe