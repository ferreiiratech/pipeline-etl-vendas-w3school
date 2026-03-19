import pandas as pd

from src.transform.core.base import BaseTransformer
from src.transform.core.validation import (
    coerce_numeric,
    ensure_positive,
    ensure_unique,
)


class OrderDetailsTransformer(BaseTransformer):
    dataset_name = "order_details"
    log_name = "Order details"
    required_columns = {"orderdetailid", "orderid", "productid", "quantity"}
    rename_mapping = {
        "orderdetailid": "id",
        "orderid": "order_id",
        "productid": "product_id",
        "quantity": "quantity",
    }

    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        for id_column in ["orderdetailid", "productid", "orderid", "quantity"]:
            coerce_numeric(dataframe, id_column)
            ensure_positive(dataframe, id_column)

        ensure_unique(dataframe, "orderdetailid")

        coerce_numeric(dataframe, "quantity")
        return dataframe

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["id"] = dataframe["id"].astype("int64")
        dataframe["order_id"] = dataframe["order_id"].astype("int64")
        dataframe["product_id"] = dataframe["product_id"].astype("int64")
        dataframe["quantity"] = dataframe["quantity"].astype("int64")
        return dataframe
