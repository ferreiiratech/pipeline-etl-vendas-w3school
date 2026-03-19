import logging
import os
from datetime import datetime, timezone
import pandas as pd

class QuarantineCsvRepository:
    """CSV persistence gateway for quarantine records."""

    def __init__(self, output_dir: str = "data/quarantine") -> None:
        self.output_dir = output_dir

    def send(self, df: pd.DataFrame, table_name: str, reason: str) -> None:
        """Persist invalid records to the quarantine layer with metadata columns.

        Records are appended to an existing quarantine file when one already exists,
        so multiple violations for the same table accumulate in a single file.
        """
        if df.empty: return

        os.makedirs(self.output_dir, exist_ok=True)

        quarantine_df = df.copy()
        quarantine_df["_quarantine_reason"] = reason
        quarantine_df["_quarantine_timestamp"] = datetime.now(tz=timezone.utc).isoformat()

        output_path = os.path.join(self.output_dir, f"{table_name}.csv")
        if os.path.exists(output_path):
            existing = pd.read_csv(output_path)
            quarantine_df = pd.concat([existing, quarantine_df], ignore_index=True)

        quarantine_df.to_csv(output_path, index=False)
        logging.warning(
            "Quarantined %s record(s) from '%s': %s",
            len(df),
            table_name,
            reason,
        )

