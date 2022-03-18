from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_filters import RowFilter, ValueRangeFilter
import itertools
from typing import List


def compose(
    table_catalog: dict, identifiers_mapping: dict
) -> (RowSet, List[RowFilter]):
    row_key_identifiers = table_catalog["row_key_identifiers"]
    row_key_separator = table_catalog["row_key_separator"]

    _validate(row_key_identifiers, identifiers_mapping)
    row_set = _compose_row_set(
        row_key_identifiers, identifiers_mapping, row_key_separator
    )
    predicate_filters = _compose_predicate_filters(
        row_key_identifiers, identifiers_mapping
    )
    return row_set, predicate_filters


def _validate(row_key_identifiers, identifiers_mapping):
    for identifier in row_key_identifiers:
        if identifier not in identifiers_mapping:
            raise Exception(f"row_key partition: {identifier} is required")

    for identifier in row_key_identifiers[:-1]:
        row_range = identifiers_mapping[identifier]
        if not isinstance(row_range, list):
            raise Exception(
                f"row_key partition: prefix {identifier} only support '=' and 'IN'"
            )

    identifier = row_key_identifiers[-1]
    last_row_range = identifiers_mapping[identifier]
    if not isinstance(last_row_range, tuple) and not isinstance(last_row_range, list):
        raise Exception(
            f"row_key partition: current operation on {identifier} is not supported"
        )


def _compose_row_set(row_key_identifiers, identifiers_mapping, row_key_separator):
    prefixes = [""]
    for identifier in row_key_identifiers[:-1]:
        row_range = identifiers_mapping[identifier]
        prefixes = list(
            map(
                lambda p: p[0] + p[1] + row_key_separator,
                [p for p in itertools.product(prefixes, row_range)],
            )
        )

    identifier = row_key_identifiers[-1]
    last_row_range = identifiers_mapping[identifier]
    row_set = RowSet()
    if isinstance(last_row_range, tuple):
        start_key, end_key, start_inclusive, end_inclusive = last_row_range
        for prefix in prefixes:
            row_set.add_row_range_from_keys(
                start_key=_encode_cell_value(f"{prefix}{start_key}"),
                start_inclusive=start_inclusive,
                end_key=_encode_cell_value(f"{prefix}{end_key}"),
                end_inclusive=end_inclusive,
            )
    else:
        # list
        for row_key in map(
            lambda p: p[0] + p[1],
            [p for p in itertools.product(prefixes, last_row_range)],
        ):
            row_set.add_row_key(row_key)
    return row_set


def _compose_predicate_filters(row_key_identifiers, identifiers_mapping):
    predicate_filters = []

    for identifier, value in identifiers_mapping.items():
        if identifier in row_key_identifiers:
            continue
        if isinstance(value, tuple):
            start_value, end_value, inclusive_start, inclusive_end = value
            f = ValueRangeFilter(
                start_value=_encode_cell_value(start_value),
                end_value=_encode_cell_value(end_value),
                inclusive_start=inclusive_start,
                inclusive_end=inclusive_end,
            )
            predicate_filters.append(f)
        elif len(value) == 1:
            v = value[0]
            f = ValueRangeFilter(
                start_value=_encode_cell_value(v),
                end_value=_encode_cell_value(v),
                inclusive_start=True,
                inclusive_end=True,
            )
            predicate_filters.append(f)

    return predicate_filters


def _encode_cell_value(cell_value):
    if cell_value is None:
        return None

    if isinstance(cell_value, str):
        return cell_value.encode()
    if isinstance(cell_value, int):
        # python bigtable SDK encode for us
        return cell_value
    raise Exception(f"compose: {cell_value} only supports string and integer")
