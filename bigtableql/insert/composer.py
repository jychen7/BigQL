from google.cloud.bigtable.row import DirectRow
from typing import List
import struct, itertools
from bigql.select import scanner


def compose(table_catalog, column_family_id, keys, values_batch) -> List[DirectRow]:
    # keys = ["a", "b"]
    # values_batch = [[1,2], [3,4]]
    # => [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
    key_value_batch = [
        dict(zip(k, v)) for (k, v) in itertools.product([keys], values_batch)
    ]

    int_qualifiers = scanner._int_qualifiers(
        set(keys), table_catalog["column_families"][column_family_id].get("columns", {})
    )
    row_key_identifiers = table_catalog["row_key_identifiers"]

    rows = []
    for key_value in key_value_batch:
        row_key = (
            table_catalog["row_key_separator"]
            .join([key_value[identifier] for identifier in row_key_identifiers])
            .encode()
        )
        row = DirectRow(row_key)

        qualifiers = set(key_value.keys()) - set(row_key_identifiers)
        for qualifier in qualifiers:
            row.set_cell(
                column_family_id,
                qualifier.encode(),
                _encode_value(key_value[qualifier], qualifier in int_qualifiers),
            )
        rows.append(row)
    return rows


def _encode_value(value, is_int):
    if is_int:
        return struct.Struct(">q").pack(int(value))
    return value.encode()
