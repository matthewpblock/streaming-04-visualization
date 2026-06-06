"""src/streaming/data_validation/data_contract_nba.py.

Defines what a valid message looks like for the NBA events stream:
required fields, allowed values, and output field order.
"""

from typing import Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import (
    validate_boolean_text,
    validate_datetime,
    validate_required_fields,
)

# === REQUIRED FIELDS ===

NBA_REQUIRED_FIELDS: Final[list[str]] = [
    "play_id",
    "game_id",
    "timestamp",
    "player_id",
    "action_type",
    "shot_type",
    "distance_ft",
    "is_made",
    "quarter",
    "time_remaining",
]

CONSUMED_FIELDNAMES: Final[list[str]] = [
    *NBA_REQUIRED_FIELDS,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]


def validate_nba_record(record: DataRecordDict) -> ValidationResult:
    """Validate one NBA event record against the data contract."""
    errors: list[str] = []

    errors.extend(
        validate_required_fields(record=record, required_fields=NBA_REQUIRED_FIELDS)
    )
    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    errors.extend(validate_datetime(record["timestamp"]))
    errors.extend(validate_boolean_text(record["is_made"], field_name="is_made"))

    has_errors = bool(errors)
    return ValidationResult(is_valid=not has_errors, errors=errors)
