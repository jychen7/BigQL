from bigql import RESERVED_TIMESTAMP, SELECT_STAR
from bigql.select import parser, validator, composer, scanner, executor
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
            identifiers_mapping,
        ) = parser.parse(self.select, self.catalog)

        # parser have check validate_table_name
        table_catalog = self.catalog[table_name]
        validator.validate_column_family(
            table_name, table_catalog, self.column_family_id
        )

        qualifiers, non_qualifiers = self._split_qualifiers(
            table_catalog, projection, selection
        )

        validator.validate_columns(
            table_name, table_catalog, self.column_family_id, qualifiers
        )

        row_set, predicate_filters = composer.compose(
            table_catalog, identifiers_mapping
        )
        record_batch = scanner.scan(
            self.bigtable_client,
            table_catalog,
            self.column_family_id,
            row_set,
            predicate_filters,
            qualifiers,
            non_qualifiers,
        )
        return executor.execute(table_name, record_batch, self.sql)

    def _split_qualifiers(self, table_catalog, projection, selection):
        non_qualifiers = set(table_catalog["row_key_identifiers"]) | {
            RESERVED_TIMESTAMP
        }
        if SELECT_STAR in projection:
            qualifiers = set(
                table_catalog["column_families"][self.column_family_id][
                    "columns"
                ].keys()
            )
        else:
            qualifiers = (projection | selection) - non_qualifiers
        return qualifiers, non_qualifiers
