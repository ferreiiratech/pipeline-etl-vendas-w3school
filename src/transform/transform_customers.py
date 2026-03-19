import pandas as pd

from src.transform.core.base import BaseTransformer
from src.transform.core.normalization import POSTAL_CODE_PATTERN_BR
from src.transform.core.validation import (
    coerce_numeric,
    ensure_non_empty_strings,
    ensure_positive,
    ensure_regex_pattern,
    ensure_unique,
)


class CustomersTransformer(BaseTransformer):
    dataset_name = "customers"
    log_name = "Customers"
    required_columns = {
        "customerid",
        "customername",
        "contactname",
        "address",
        "city",
        "postalcode",
        "country",
    }
    rename_mapping = {
        "customerid": "id",
        "customername": "name",
        "contactname": "contact_name",
        "postalcode": "postal_code",
    }

    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        coerce_numeric(dataframe, "customerid")
        ensure_positive(dataframe, "customerid")
        ensure_unique(dataframe, "customerid")
        ensure_non_empty_strings(
            dataframe,
            ["customername", "contactname", "address", "city", "country", "postalcode"],
        )
        ensure_regex_pattern(
            dataframe,
            "postalcode",
            POSTAL_CODE_PATTERN_BR,
            "Column 'postalcode' contains values that do not match the expected format (XXXXX-XXX): {values}",
        )
        return dataframe

    def normalize_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["contactname"] = dataframe["contactname"].fillna("").astype(str).str.strip()
        dataframe.loc[dataframe["contactname"].eq(""), "contactname"] = "No contact name provided"

        dataframe["customername"] = dataframe["customername"].astype(str).str.strip()
        for col in ["address", "city", "country", "customername"]:
            dataframe[col] = dataframe[col].astype(str).str.strip().str.title()

        return dataframe

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["id"] = dataframe["id"].astype("int64")
        return dataframe