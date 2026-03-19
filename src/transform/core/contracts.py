from abc import ABC, abstractmethod

import pandas as pd


class Transformer(ABC):
    """Contract for all dataframe transformers."""

    @abstractmethod
    def validate(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Validate source dataframe before transformation."""

    @abstractmethod
    def transform(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Return transformed dataframe for silver layer."""
