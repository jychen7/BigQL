from typing import List, Tuple
from google.rpc.status_pb2 import Status

SUCCESS = 0


def write(bigtable_client, table_catalog, rows) -> List[Tuple[bool, Status]]:
    bigtable_table = bigtable_client.instance(table_catalog["instance_id"]).table(
        table_catalog["table_name"]
    )
    statues = bigtable_table.mutate_rows(rows)
    return [(s.code == SUCCESS, s) for s in statues]
