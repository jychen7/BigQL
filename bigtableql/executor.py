import pyarrow
import datafusion
from typing import List


def execute(
    table_name: str, record_batch: pyarrow.RecordBatch, sql: str
) -> List[pyarrow.RecordBatch]:
    ctx = datafusion.ExecutionContext()
    ctx.register_record_batches(table_name, [[record_batch]])
    answers = ctx.sql(sql).collect()
    return answers
