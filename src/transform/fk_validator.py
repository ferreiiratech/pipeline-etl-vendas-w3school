import logging
from dataclasses import dataclass
from typing import Callable, Sequence, Tuple

import pandas as pd


def validate_foreign_key(
    df: pd.DataFrame,
    fk_col: str,
    ref_df: pd.DataFrame,
    ref_col: str,
    table_name: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Check referential integrity between df[fk_col] and ref_df[ref_col].

    Args:
        df: DataFrame whose foreign key column is being validated.
        fk_col: Column name in df that holds the foreign key values.
        ref_df: Reference DataFrame that owns the primary key.
        ref_col: Column name in ref_df that holds the primary key values.
        table_name: Human-readable name used in log messages.

    Returns:
        (valid_df, orphan_df) — rows that satisfy the FK constraint and rows
        whose FK value was not found in the reference table.
    """
    valid_keys = set(ref_df[ref_col])
    is_orphan = ~df[fk_col].isin(valid_keys)
    orphan_count = int(is_orphan.sum())

    if orphan_count:
        logging.warning(
            "FK violation in '%s': %s orphan row(s) where '%s' not found in referenced table.",
            table_name,
            orphan_count,
            fk_col,
        )

    return df[~is_orphan].copy(), df[is_orphan].copy()


@dataclass(frozen=True)
class ForeignKeyRule:
    table_name: str
    fk_column: str
    reference_table: str
    reference_column: str = "id"
    quarantine_reason: str | None = None

    def reason(self) -> str:
        if self.quarantine_reason:
            return self.quarantine_reason
        return f"{self.fk_column} not found in {self.reference_table}"


class ForeignKeyValidationService:
    """Apply FK validation rules and route invalid records to quarantine."""

    def __init__(
        self,
        quarantine_handler: Callable[[pd.DataFrame, str, str], None],
    ) -> None:
        self._quarantine_handler = quarantine_handler

    def apply_rules(
        self,
        dataframes: dict[str, pd.DataFrame],
        rules: Sequence[ForeignKeyRule],
    ) -> dict[str, pd.DataFrame]:
        for rule in rules:
            valid_df, orphan_df = validate_foreign_key(
                dataframes[rule.table_name],
                rule.fk_column,
                dataframes[rule.reference_table],
                rule.reference_column,
                rule.table_name,
            )
            dataframes[rule.table_name] = valid_df
            self._quarantine_handler(orphan_df, rule.table_name, rule.reason())

        return dataframes
