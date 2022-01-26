from google.cloud.bigtable.row_set import RowSet
import itertools


def compose(table_catalog: dict, row_key_identifiers_mapping: dict) -> RowSet:
    row_key_identifiers = table_catalog["row_key_identifiers"]
    row_key_separator = table_catalog["row_key_separator"]

    row_set = RowSet()
    prefixes = [""]
    for identifier in row_key_identifiers:
        if identifier not in row_key_identifiers_mapping:
            raise Exception(f"row_key partition: {identifier} is required")

    for identifier in row_key_identifiers[:-1]:
        row_range = row_key_identifiers_mapping[identifier]
        if not isinstance(row_range, list):
            raise Exception(
                f"row_key partition: prefix {identifier} only support '=' and 'IN'"
            )
        prefixes = list(
            map(
                lambda p: p[0] + p[1] + row_key_separator,
                [p for p in itertools.product(prefixes, row_range)],
            )
        )

    identifier = row_key_identifiers[-1]
    last_row_range = row_key_identifiers_mapping[identifier]
    if isinstance(last_row_range, tuple):
        start_key, end_key = last_row_range
        for prefix in prefixes:
            row_set.add_row_range_from_keys(
                start_key=f"{prefix}{start_key}".encode(),
                start_inclusive=True,
                end_key=f"{prefix}{end_key}".encode(),
                end_inclusive=True,
            )
    elif isinstance(last_row_range, list):
        for row_key in map(
            lambda p: p[0] + p[1],
            [p for p in itertools.product(prefixes, last_row_range)],
        ):
            row_set.add_row_key(row_key)
    else:
        raise Exception(
            f"row_key partition: current operation on {identifier} is not supported"
        )

    return row_set
