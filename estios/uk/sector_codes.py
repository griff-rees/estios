from typing import Final

from pandas import Series

from .input_output_tables import InputOutputTableUK2017


def get_uk_io_codes() -> tuple[dict[str, str], dict[str, str]]:
    io_table_2017 = InputOutputTableUK2017()

    UK_INPUT_CODES: Final[Series] = io_table_2017.all_input_rows
    UK_INPUT_LABELS: Final[Series] = io_table_2017.all_input_row_labels

    UK_OUTPUT_CODES: Final[Series] = io_table_2017.all_output_columns
    UK_OUTPUT_LABELS: Final[Series] = io_table_2017.all_output_column_labels
    input_codes = {}
    output_codes = {}
    for k, v in zip(UK_INPUT_CODES, UK_INPUT_LABELS):
        input_codes[k] = v.strip()
    for k, v in zip(UK_OUTPUT_CODES, UK_OUTPUT_LABELS):
        output_codes[k] = v.strip()
    return input_codes, output_codes
