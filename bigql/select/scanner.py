import pyarrow
from google.cloud.bigtable.row_set import RowSet
from google.cloud.bigtable.row_data import PartialRowData
from google.cloud.bigtable.row_filters import (
    RowFilter,
    RowFilterChain,
    FamilyNameRegexFilter,
    CellsColumnLimitFilter,
    ColumnQualifierRegexFilter,
)
from bigql import RESERVED_TIMESTAMP
from typing import List


def scan(
    bigtable_client,
    table_catalog: dict,
    column_family_id: str,
    row_set: RowSet,
    predicate_filters: List[RowFilter],
    qualifiers: set,
    non_qualifiers: set,
) -> pyarrow.RecordBatch:
    column_family = table_catalog["column_families"][column_family_id]
    row_key_identifiers = table_catalog["row_key_identifiers"]
    row_key_separator = table_catalog["row_key_separator"]
    table_name = table_catalog["table_name"]

    int_qualifiers = _int_qualifiers(qualifiers, column_family.get("columns", {}))

    bigtable_table = bigtable_client.instance(table_catalog["instance_id"]).table(
        table_name
    )

    row_filter = RowFilterChain(
        _row_chain(column_family_id, column_family, qualifiers, predicate_filters)
    )

    columnar = {q: [] for q in qualifiers}
    columnar.update({q: [] for q in non_qualifiers})

    for row_data in bigtable_table.read_rows(row_set=row_set, filter_=row_filter):
        _process_row(
            columnar,
            row_data,
            row_key_identifiers,
            row_key_separator,
            column_family_id,
            qualifiers,
            int_qualifiers,
        )

    schema = pyarrow.schema(
        [
            (
                k,
                pyarrow.int64()
                if k in int_qualifiers
                else (
                    pyarrow.float64() if k == RESERVED_TIMESTAMP else pyarrow.string()
                ),
            )
            for k in columnar.keys()
        ]
    )
    record_batch = pyarrow.RecordBatch.from_pydict(columnar, schema=schema)
    return record_batch


def _process_row(
    columnar: dict,
    row_data: PartialRowData,
    row_key_identifiers: set,
    row_key_separator: str,
    column_family_id: str,
    qualifiers: set,
    int_qualifiers: set,
):
    if len(row_key_identifiers) == 1:
        row_key_values = [row_data.row_key.decode()]
    else:
        row_key_values = row_data.row_key.decode().split(row_key_separator)

    by_timestamp = {}
    for qualifier in qualifiers:
        for cell in row_data.cells[column_family_id][qualifier.encode()]:
            cell_timestamp = cell.timestamp.timestamp()
            if cell_timestamp not in by_timestamp:
                by_timestamp[cell_timestamp] = {}
            by_timestamp[cell_timestamp][qualifier] = _decode_cell_value(
                cell.value, qualifier in int_qualifiers
            )

    for ts, ts_qualifiers in by_timestamp.items():
        for qualifier in qualifiers:
            columnar[qualifier].append(ts_qualifiers.get(qualifier, None))
        for i, composite_row_key in enumerate(row_key_identifiers):
            columnar[composite_row_key].append(row_key_values[i])
        columnar[RESERVED_TIMESTAMP].append(ts)


def _int_qualifiers(qualifiers: set, columns_map: dict) -> set:
    return qualifiers & {
        col if col_type == int else None for col, col_type in columns_map.items()
    }


def _row_chain(
    column_family_id: str, column_family: dict, qualifiers: set, predicate_filters
):
    chain = [
        FamilyNameRegexFilter(column_family_id),
        # projection pushdown
        ColumnQualifierRegexFilter("|".join(qualifiers)),
    ]
    if column_family.get("only_read_latest"):
        chain.append(CellsColumnLimitFilter(1))
    # predicate pushdown not allow filter on one qualifier, it works on all qualifiers' cells
    # chain += predicate_filters
    return chain


def _decode_cell_value(cell_value, is_int):
    if cell_value is None:
        return None

    if is_int:
        return int.from_bytes(cell_value, byteorder="big")
    return cell_value.decode()
