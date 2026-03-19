import pandas as pd

from src.transform.core.base import BaseTransformer
from src.transform.core.validation import (
    coerce_numeric,
    ensure_non_empty_strings,
    ensure_non_negative,
    ensure_positive,
    ensure_unique,
)


class ProductsTransformer(BaseTransformer):
    dataset_name = "products"
    log_name = "Products"
    required_columns = {
        "productid",
        "productname",
        "supplierid",
        "categoryid",
        "unit",
        "price",
    }
    rename_mapping = {
        "productid": "id",
        "productname": "product_name",
        "supplierid": "supplier_id",
        "categoryid": "category_id",
    }

    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        for id_column in ["productid", "supplierid", "categoryid"]:
            coerce_numeric(dataframe, id_column)
            ensure_positive(dataframe, id_column)

        ensure_unique(dataframe, "productid")
        ensure_non_empty_strings(dataframe, ["productname", "unit"])

        coerce_numeric(dataframe, "price")
        ensure_non_negative(dataframe, "price")
        return dataframe

    def normalize_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["productname"] = dataframe["productname"].astype(str).str.strip().str.title()
        dataframe["unit"] = dataframe["unit"].astype(str).str.strip()
        return dataframe

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        for id_column in ["id", "supplier_id", "category_id"]:
            dataframe[id_column] = dataframe[id_column].astype("int64")
        dataframe["price"] = dataframe["price"].astype("float64")
        return dataframe
