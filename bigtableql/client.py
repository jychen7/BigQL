from google.cloud import bigtable
import pyarrow
import sqloxide
from typing import List
from bigtableql import select_parser, insert_parser, composer, scanner, executor, writer
from bigtableql import (
    RESERVED_ROWKEY,
    RESERVED_TIMESTAMP,
    DEFAULT_SEPARATOR,
    SELECT_STAR,
)


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

    def query(self, column_family_id: str, sql: str):
        parsed = sqloxide.parse_sql(sql=sql, dialect="ansi")[0]
        if "Insert" in parsed:
            return self._insert(column_family_id, parsed["Insert"])
        return self._select(column_family_id, parsed["Query"]["body"]["Select"], sql)

    def _select(
        self, column_family_id: str, select, sql: str
    ) -> List[pyarrow.RecordBatch]:
        (
            table_name,
            projection,
            selection,
            row_key_identifiers_mapping,
        ) = select_parser.parse(select, self.catalog)
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

        columns = table_catalog["column_families"][column_family_id]["columns"].keys()
        if SELECT_STAR in projection:
            qualifiers = set(columns)
        else:
            qualifiers = (projection | selection) - non_qualifiers

        for qualifier in qualifiers:
            if qualifier not in columns:
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

    def _insert(self, column_family_id: str, insert):
        table_name, keys, values_batch = insert_parser.parse(insert)

        if table_name not in self.catalog:
            raise Exception(
                f"catalog: {table_name} not found, please register_table first"
            )
        table_catalog = self.catalog[table_name]

        if column_family_id not in table_catalog["column_families"]:
            raise Exception(
                f"table {table_name}: column_family {column_family_id} not found"
            )

        row_key_identifiers = table_catalog["row_key_identifiers"]
        columns = table_catalog["column_families"][column_family_id]["columns"].keys()
        unregisterd_keys = set(keys) - (set(row_key_identifiers) | set(columns))
        if unregisterd_keys:
            raise Exception(f"insert: {unregisterd_keys} not registered")
        missing_identifiers = set(row_key_identifiers) - set(keys)
        if missing_identifiers:
            raise Exception(f"insert: {missing_identifiers} required")

        n = len(keys)
        for values in values_batch:
            if len(values) != n:
                raise Exception(
                    f"insert: {values} is invalid, should have {n} elements"
                )

        return writer.write(
            self.bigtable_client, table_catalog, column_family_id, keys, values_batch
        )
