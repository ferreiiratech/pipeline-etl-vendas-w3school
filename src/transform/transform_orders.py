import pandas as pd

from src.transform.core.base import BaseTransformer
from src.transform.core.validation import (
    coerce_numeric,
    ensure_datetime,
    ensure_positive,
    ensure_unique,
)


class OrdersTransformer(BaseTransformer):
    dataset_name = "orders"
    log_name = "Orders"
    required_columns = {"orderid", "customerid", "employeeid", "orderdate", "shipperid"}
    rename_mapping = {
        "orderid": "id",
        "customerid": "customer_id",
        "employeeid": "employee_id",
        "orderdate": "order_date",
        "shipperid": "shipper_id",
    }

    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        for id_column in ["orderid", "customerid", "employeeid", "shipperid"]:
            coerce_numeric(dataframe, id_column)
            ensure_positive(dataframe, id_column)

        ensure_unique(dataframe, "orderid")
        ensure_datetime(dataframe, "orderdate")
        return dataframe

    def normalize_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["orderdate"] = pd.to_datetime(dataframe["orderdate"], errors="coerce")
        return dataframe

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        for id_column in ["id", "customer_id", "employee_id", "shipper_id"]:
            dataframe[id_column] = dataframe[id_column].astype("int64")
        return dataframe
