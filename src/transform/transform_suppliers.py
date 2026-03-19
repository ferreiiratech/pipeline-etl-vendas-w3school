import pandas as pd

from src.transform.core.base import BaseTransformer
from src.transform.core.normalization import (
    PHONE_PATTERN_BR,
    POSTAL_CODE_PATTERN_BR,
    format_phone_br,
)
from src.transform.core.validation import (
    coerce_numeric,
    ensure_non_empty_strings,
    ensure_positive,
    ensure_regex_pattern,
    ensure_unique,
)


class SuppliersTransformer(BaseTransformer):
    dataset_name = "suppliers"
    log_name = "Suppliers"
    required_columns = {
        "supplierid",
        "suppliername",
        "contactname",
        "address",
        "city",
        "postalcode",
        "country",
        "phone",
    }
    rename_mapping = {
        "supplierid": "id",
        "suppliername": "name",
        "contactname": "contact_name",
        "postalcode": "postal_code",
    }

    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        coerce_numeric(dataframe, "supplierid")
        ensure_positive(dataframe, "supplierid")
        ensure_unique(dataframe, "supplierid")
        ensure_non_empty_strings(
            dataframe,
            ["suppliername", "contactname", "address", "city", "country", "postalcode", "phone"],
        )
        return dataframe

    def normalize_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["suppliername"] = dataframe["suppliername"].astype(str).str.strip()
        dataframe["contactname"] = dataframe["contactname"].astype(str).str.strip().str.title()
        dataframe["address"] = dataframe["address"].astype(str).str.strip()
        dataframe["city"] = dataframe["city"].astype(str).str.strip().str.title()
        dataframe["country"] = dataframe["country"].astype(str).str.strip().str.title()
        dataframe["postalcode"] = dataframe["postalcode"].astype(str).str.strip().str.upper()
        dataframe["phone"] = dataframe["phone"].astype(str).str.strip().apply(format_phone_br)

        ensure_regex_pattern(
            dataframe,
            "postalcode",
            POSTAL_CODE_PATTERN_BR,
            "Invalid postal code format in 'postalcode' column: {values}",
        )
        ensure_regex_pattern(
            dataframe,
            "phone",
            PHONE_PATTERN_BR,
            "Invalid phone format in 'phone' column: {values}",
        )
        return dataframe

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["id"] = dataframe["id"].astype("int64")
        return dataframe
