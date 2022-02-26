import pyarrow
import datafusion
from typing import List


def execute(
    table_name: str, record_batch: pyarrow.RecordBatch, sql: str
) -> List[pyarrow.RecordBatch]:
    if record_batch.num_rows == 0:
        return [record_batch]

    ctx = datafusion.ExecutionContext()
    ctx.register_record_batches(table_name, [[record_batch]])
    answers = ctx.sql(sql).collect()
    return answers
