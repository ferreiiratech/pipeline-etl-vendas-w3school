import re

import pandas as pd


def require_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    dataset_name: str,
) -> pd.DataFrame:
    """Validate required schema columns and return a defensive copy."""
    missing_columns = required_columns - set(dataframe.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns in {dataset_name} DataFrame: {missing}")
    return dataframe.copy()


def coerce_numeric(dataframe: pd.DataFrame, column: str) -> None:
    dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
    if dataframe[column].isna().any():
        raise ValueError(f"Column '{column}' contains non-numeric values.")


def ensure_positive(dataframe: pd.DataFrame, column: str) -> None:
    if (dataframe[column] <= 0).any():
        raise ValueError(f"Column '{column}' must contain only positive values.")


def ensure_non_negative(dataframe: pd.DataFrame, column: str) -> None:
    if (dataframe[column] < 0).any():
        raise ValueError(f"Column '{column}' must be greater than or equal to zero.")


def ensure_unique(dataframe: pd.DataFrame, column: str) -> None:
    if dataframe[column].duplicated().any():
        duplicated_ids = dataframe.loc[dataframe[column].duplicated(), column].tolist()
        raise ValueError(f"Duplicated '{column}' values found: {duplicated_ids}")


def ensure_non_empty_strings(dataframe: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        values = dataframe[column].fillna("").astype(str).str.strip()
        if values.eq("").any():
            raise ValueError(f"Column '{column}' cannot contain null or empty values.")


def ensure_regex_pattern(
    dataframe: pd.DataFrame,
    column: str,
    pattern: re.Pattern,
    error_message: str,
) -> None:
    invalid_mask = ~dataframe[column].astype(str).str.strip().str.match(pattern)
    if invalid_mask.any():
        invalid_values = dataframe.loc[invalid_mask, column].tolist()
        raise ValueError(error_message.format(values=invalid_values))


def ensure_datetime(dataframe: pd.DataFrame, column: str) -> None:
    parsed_values = pd.to_datetime(dataframe[column], errors="coerce")
    if parsed_values.isna().any():
        raise ValueError(f"Column '{column}' contains invalid date values.")
