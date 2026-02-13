import logging
from typing import List, Dict, Any, Type, Tuple
from pydantic import BaseModel, ValidationError


def validate_records(
    records: List[Dict[str, Any]],
    contract: Type[BaseModel],
    max_error_rate: float = 0.05,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validate a list of records against a Pydantic contract model.

    Args:
        records: List of dictionaries to validate.
        contract: Pydantic model class to validate against.
        max_error_rate: Maximum allowed fraction of invalid records (0.0 to 1.0).
            If exceeded, raises ValueError.

    Returns:
        Tuple of (valid_records, validation_errors).
        valid_records: list of original dicts that passed validation.
        validation_errors: list of dicts with 'index', 'record', and 'errors' keys.
    """
    if not records:
        return [], []

    valid_records: List[Dict[str, Any]] = []
    validation_errors: List[Dict[str, Any]] = []

    for i, record in enumerate(records):
        try:
            contract.model_validate(record)
            valid_records.append(record)
        except ValidationError as e:
            error_detail = {
                "index": i,
                "record": record,
                "errors": e.errors(),
            }
            validation_errors.append(error_detail)
            logging.warning(
                f"Validation error at index {i}: {e.error_count()} issue(s)"
            )

    total = len(records)
    error_count = len(validation_errors)
    error_rate = error_count / total

    logging.info(
        f"Validation complete: {len(valid_records)}/{total} valid, "
        f"{error_count}/{total} invalid ({error_rate:.1%})"
    )

    if error_rate > max_error_rate:
        sample_errors = validation_errors[:3]
        error_summary = "; ".join(
            f"index {e['index']}: {e['errors']}" for e in sample_errors
        )
        raise ValueError(
            f"Validation error rate {error_rate:.1%} exceeds threshold "
            f"{max_error_rate:.0%}. Sample errors: {error_summary}"
        )

    return valid_records, validation_errors


def validate_single(
    record: Dict[str, Any],
    contract: Type[BaseModel],
) -> Dict[str, Any]:
    """
    Validate a single record against a Pydantic contract model.

    Args:
        record: Dictionary to validate.
        contract: Pydantic model class to validate against.

    Returns:
        The original record dict if valid.

    Raises:
        ValidationError: If the record fails validation.
    """
    contract.model_validate(record)
    return record


def format_validation_summary(
    total: int,
    valid_count: int,
    errors: List[Dict[str, Any]],
) -> str:
    """
    Format a human-readable validation summary for logging or notifications.

    Args:
        total: Total number of records validated.
        valid_count: Number of records that passed validation.
        errors: List of validation error dicts from validate_records.

    Returns:
        Formatted summary string.
    """
    error_count = len(errors)
    error_rate = error_count / total if total > 0 else 0.0

    lines = [
        f"Validated {total} records: {valid_count} valid, {error_count} invalid ({error_rate:.1%})",
    ]

    for error in errors[:5]:
        field_errors = ", ".join(f"{e['loc'][-1]}: {e['msg']}" for e in error["errors"])
        lines.append(f"  - Index {error['index']}: {field_errors}")

    if error_count > 5:
        lines.append(f"  ... and {error_count - 5} more errors")

    return "\n".join(lines)
