import pandas as pd

from src.transform.core.base import BaseTransformer
from src.transform.core.validation import (
    coerce_numeric,
    ensure_datetime,
    ensure_non_empty_strings,
    ensure_positive,
    ensure_unique,
)


class EmployeesTransformer(BaseTransformer):
    dataset_name = "employees"
    log_name = "Employees"
    required_columns = {"employeeid", "lastname", "firstname", "birthdate", "photo", "notes"}
    rename_mapping = {
        "employeeid": "id",
        "lastname": "last_name",
        "firstname": "first_name",
        "birthdate": "birth_date",
        "photo": "photo_url",
        "notes": "notes",
    }

    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        coerce_numeric(dataframe, "employeeid")
        ensure_positive(dataframe, "employeeid")
        ensure_unique(dataframe, "employeeid")
        ensure_non_empty_strings(dataframe, ["lastname", "firstname", "birthdate", "photo"])
        ensure_datetime(dataframe, "birthdate")
        return dataframe

    def normalize_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["notes"] = dataframe["notes"].fillna("No notes provided").astype(str).str.strip()

        for col in ["lastname", "firstname"]:
            dataframe[col] = dataframe[col].astype(str).str.strip().str.title()

        dataframe["photo"] = dataframe["photo"].astype(str).str.strip()
        dataframe["birthdate"] = pd.to_datetime(dataframe["birthdate"], errors="coerce")
        return dataframe

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["id"] = dataframe["id"].astype("int64")
        return dataframe
