"""Utility functions for schema validation."""

from pandera.errors import SchemaError
from collections.abc import Callable

def schema_error_handler(func: Callable) -> Callable:
    """Custom error handler for the schema validation errors.

    Use as decorator to wrap pa.check_types decorator.
    """

    def wrapper(*args, **kwargs):  # noqa: ANN201, ANN002, ANN003
        try:
            return func(*args, **kwargs)
        except SchemaError as e:
            e_dict = vars(e)
            err_msg = "Error validating the raw routes DataFrame."
            schema = e_dict.get("schema")
            reason_code = e_dict.get("reason_code")
            column_name = e_dict.get("column_name")
            check = e_dict.get("check")
            failure_cases = e_dict.get("failure_cases")
            if schema:
                err_msg += f"\nSchema: {schema}"
            if reason_code:
                err_msg += f"\nReason code: {reason_code}"
            if column_name:
                err_msg += f"\nColumn name: {column_name}"
            if check:
                err_msg += f"\nCheck: {check}"

            raise SchemaError(schema=schema, data=failure_cases, message=err_msg) from e
        except Exception as e:
            raise e

    return wrapper