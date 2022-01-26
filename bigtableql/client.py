from google.cloud import bigtable
import pyarrow
from typing import List
from bigtableql import parser, composer, scanner, executor
from bigtableql import RESERVED_ROWKEY, RESERVED_TIMESTAMP, DEFAULT_SEPARATOR


class Client:
    def __init__(self, *args, **kwargs):
        self.bigtable_client = bigtable.client.Client(*args, **kwargs)
        self.catalog = {}

    def register_table(
        self,
        table_name: str,
        instance_id: str,
        column_families: dict,
        row_key_identifiers=[RESERVED_ROWKEY],
        row_key_separator=DEFAULT_SEPARATOR,
    ):
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#bigtablecolumnfamily
        # column_families = {
        #     "profile": {
        #         "only_read_latest": True,
        #         "columns": {
        #             "gender": str,
        #             "age": int
        #         }
        #     }
        # }
        self.catalog[table_name] = {
            "table_name": table_name,
            "instance_id": instance_id,
            "column_families": column_families,
            "row_key_identifiers": row_key_identifiers,
            "row_key_separator": row_key_separator,
        }

    def query(self, column_family_id: str, sql: str) -> List[pyarrow.RecordBatch]:
        table_name, projection, selection, row_key_identifiers_mapping = parser.parse(
            sql, self.catalog
        )
        row_set = composer.compose(
            self.catalog[table_name], row_key_identifiers_mapping
        )

        table_catalog = self.catalog[table_name]
        if column_family_id not in table_catalog["column_families"]:
            raise Exception(
                f"table {table_name}: column_family {column_family_id} not found"
            )

        row_key_identifiers = table_catalog["row_key_identifiers"]
        non_qualifiers = set(row_key_identifiers) | {RESERVED_TIMESTAMP}
        qualifiers = (projection | selection) - non_qualifiers

        for qualifier in qualifiers:
            if (
                qualifier
                not in table_catalog["column_families"][column_family_id]["columns"]
            ):
                raise Exception(
                    f"table {table_name}, column_family {column_family_id}: {qualifier} not found"
                )

        record_batch = scanner.scan(
            self.bigtable_client,
            table_catalog,
            column_family_id,
            row_set,
            qualifiers,
            non_qualifiers,
        )
        return executor.execute(table_name, record_batch, sql)
