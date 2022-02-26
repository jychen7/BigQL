from google.cloud import bigtable
import sqloxide
from bigql.select import SelectQuery
from bigql.insert import InsertQuery
from bigql import (
    RESERVED_ROWKEY,
    DEFAULT_SEPARATOR,
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
            return InsertQuery(
                self.bigtable_client, self.catalog, column_family_id, parsed["Insert"]
            ).execute()
        return SelectQuery(
            self.bigtable_client,
            self.catalog,
            column_family_id,
            parsed["Query"]["body"]["Select"],
            sql,
        ).execute()
