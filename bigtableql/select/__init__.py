from bigtableql import RESERVED_TIMESTAMP, SELECT_STAR
from bigtableql.select import parser, composer, scanner, executor
from typing import List
import pyarrow


class SelectQuery:
    def __init__(
        self,
        bigtable_client,
        catalog: dict,
        column_family_id: str,
        select: dict,
        sql: str,
    ):
        self.bigtable_client = bigtable_client
        self.catalog = catalog
        self.column_family_id = column_family_id
        self.select = select
        self.sql = sql

    def execute(self) -> List[pyarrow.RecordBatch]:
        (
            table_name,
            projection,
            selection,
            row_key_identifiers_mapping,
        ) = parser.parse(self.select, self.catalog)
        row_set = composer.compose(
            self.catalog[table_name], row_key_identifiers_mapping
        )

        table_catalog = self.catalog[table_name]
        if self.column_family_id not in table_catalog["column_families"]:
            raise Exception(
                f"table {table_name}: column_family {self.column_family_id} not found"
            )

        row_key_identifiers = table_catalog["row_key_identifiers"]
        non_qualifiers = set(row_key_identifiers) | {RESERVED_TIMESTAMP}

        columns = table_catalog["column_families"][self.column_family_id][
            "columns"
        ].keys()
        if SELECT_STAR in projection:
            qualifiers = set(columns)
        else:
            qualifiers = (projection | selection) - non_qualifiers

        for qualifier in qualifiers:
            if qualifier not in columns:
                raise Exception(
                    f"table {table_name}, column_family {self.column_family_id}: {qualifier} not found"
                )

        record_batch = scanner.scan(
            self.bigtable_client,
            table_catalog,
            self.column_family_id,
            row_set,
            qualifiers,
            non_qualifiers,
        )
        return executor.execute(table_name, record_batch, sql)
