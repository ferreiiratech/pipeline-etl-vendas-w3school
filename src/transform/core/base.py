import logging
from abc import abstractmethod

import pandas as pd

from src.transform.core.contracts import Transformer
from src.transform.core.validation import require_columns


class BaseTransformer(Transformer):
    """Template Method base class for concrete entity transformers."""

    dataset_name: str = "dataset"
    log_name: str = "Dataset"
    required_columns: set[str] = set()
    rename_mapping: dict[str, str] = {}

    def validate(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        validated = require_columns(dataframe, self.required_columns, self.dataset_name)
        return self.validate_domain(validated)

    @abstractmethod
    def validate_domain(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Entity-specific business validation rules."""

    def normalize_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        return dataframe

    def rename_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if self.rename_mapping:
            dataframe.rename(columns=self.rename_mapping, inplace=True)
        return dataframe

    def cast_fields(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        return dataframe

    def validate_output(self, dataframe: pd.DataFrame) -> None:
        """Optional final checks after all transformations."""

    def transform(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        transformed = self.validate(dataframe)
        transformed = self.normalize_fields(transformed)
        transformed = self.rename_fields(transformed)
        transformed = self.cast_fields(transformed)
        self.validate_output(transformed)

        logging.info("%s data transformed successfully with %s rows.", self.log_name, len(transformed))
        return transformed
