from bigtableql.insert import parser, writer
from typing import List, Tuple
from google.rpc.status_pb2 import Status


class InsertQuery:
    def __init__(
        self, bigtable_client, catalog: dict, column_family_id: str, insert: dict
    ):
        self.bigtable_client = bigtable_client
        self.catalog = catalog
        self.column_family_id = column_family_id
        self.insert = insert

    def execute(self) -> List[Tuple[bool, Status]]:
        table_name, keys, values_batch = parser.parse(self.insert)

        if table_name not in self.catalog:
            raise Exception(
                f"catalog: {table_name} not found, please register_table first"
            )
        table_catalog = self.catalog[table_name]

        if self.column_family_id not in table_catalog["column_families"]:
            raise Exception(
                f"table {table_name}: column_family {self.column_family_id} not found"
            )

        row_key_identifiers = table_catalog["row_key_identifiers"]
        columns = table_catalog["column_families"][self.column_family_id][
            "columns"
        ].keys()
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
            self.bigtable_client,
            table_catalog,
            self.column_family_id,
            keys,
            values_batch,
        )
