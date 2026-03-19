import logging
import os
import pandas as pd

class SilverCsvRepository:
    """CSV persistence gateway for silver datasets."""

    def __init__(self, output_dir: str = "data/silver") -> None:
        self.output_dir = output_dir

    def save(self, df: pd.DataFrame, table_name: str) -> None:
        os.makedirs(self.output_dir, exist_ok=True)
        output_path = os.path.join(self.output_dir, f"{table_name}.csv")
        df.to_csv(output_path, index=False)
        logging.info("Saved %s rows to silver: %s", len(df), output_path)
