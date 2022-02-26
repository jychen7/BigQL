from bigql.select import validator as select_validator
from bigql.insert import parser, validator, composer, writer
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

        select_validator.validate_table_name(self.catalog, table_name)
        table_catalog = self.catalog[table_name]

        select_validator.validate_column_family(
            table_name, table_catalog, self.column_family_id
        )
        validator.validate_columns(
            table_catalog, self.column_family_id, keys, values_batch
        )

        rows = composer.compose(
            table_catalog,
            self.column_family_id,
            keys,
            values_batch,
        )

        return writer.write(self.bigtable_client, table_catalog, rows)
