import logging
import os

import pandas as pd


def extract_file_csv(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV file and returns it as a DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted data.
    """
    df = pd.read_csv(file_path)
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    logging.info("Extracted %s rows from '%s'.", len(df), table_name)
    return df
