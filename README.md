# BigtableQL

BigtableQL provides a SQL Query Layer for [Google Cloud Bigtable](https://cloud.google.com/bigtable/docs).

## Use Cases

Cloud Bigtable is Google's fully managed NoSQL Big Data database service. Each table contains rows and columns. Each row/column intersection can contain multiple cells. Each cell contains a unique timestamped version of the data for that row and column. Thus Bigtable is often used to store time series data.

BigtableQL provides a SQL query layer to run aggregation query on Bigtable.

## Quick Start

Using the [weather balloon example data](https://cloud.google.com/bigtable/docs/schema-design-time-series#example-data) shown in [Single-timestamp unserialized](https://cloud.google.com/bigtable/docs/schema-design-time-series#unserialized) schema design

```
Row key                         pressure    temperature humidity    altitude
us-west2#3698#2021-03-05-1200   94558       9.6         61          612
us-west2#3698#2021-03-05-1201   94122       9.7         62          611
us-west2#3698#2021-03-05-1202   95992       9.5         58          602
us-west2#3698#2021-03-05-1203   96025       9.5         66          598
us-west2#3698#2021-03-05-1204   96021       9.6         63          624
```

we are able to calculate average pressure of the period by

```
from bigtableql.client import Client
# config follows offical python bigtable client
client = Client(config)

client.register_table(
    "weather_balloons",
    instance_id="INSTANCE_ID",
    column_families={
        "measurements": {
            "only_read_latest": True,
            "columns": {
                "pressure": int,
                "temperature": str,
                "humidity": int,
                "altitude": int
            }
        }
    }
)

client.query("measurements", """
SELECT avg(pressure) FROM weather_balloons
WHERE
  "_row_key" BETWEEN 'us-west2#3698#2021-03-05-1200' AND 'us-west2#3698#2021-03-05-1204'
""")
```

Or with row key decomposition

```
client.register_table(
    "weather_balloons",
    instance_id="INSTANCE_ID",
    column_families={
        "measurements": {
            "only_read_latest": True,
            "columns": {
                "pressure": int,
                "temperature": str,
                "humidity": int,
                "altitude": int
            }
        }
    },
    row_key_identifiers=["location", "balloon_id", "event_minute"],
    row_key_separator="#"
)

client.query("measurements", """
SELECT balloon_id, avg(pressure) FROM weather_balloons
WHERE
  location = 'us-west2'
  AND balloon_id IN ('3698', '3700')
  AND event_minute BETWEEN '2021-03-05-1200' AND '2021-03-05-1204'
GROUP BY 1
""")
```

The output of `query` is list of [pyarrow.RecordBatch](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#pyarrow.RecordBatch.from_pydict). It can be easily convert to python dictionary (`to_pydict`) and pandas dataframe (`to_pandas`).

## Alternative

1. [Google BigQuery external data source](https://cloud.google.com/bigquery/external-data-bigtable)

However, as of 2022-01, it
- only supports "us-central1" and "europe-west1" region
- only supports query with "rowkey"
- by default can run up to [4 concurrent queries](https://cloud.google.com/bigquery/quotas) against Bigtable external data source

## Roadmap

### SQL

- ✅ Insert Into
- ✅ Select *
- ✅ Select column(s)
- ✅ Filter (WHERE): "=", "IN", "BETWEEN", ">", ">=", "<", "<="
- ✅ GROUP BY
- ✅ ORDER BY
- ✅ HAVING
- ✅ LIMIT
- ✅ Aggregate (e.g. avg, sum, count)
- ✅ AND
- ✅ Alias
- ✅ Cast
- ✅ Common Math Functions
- [ ] Common Date/Time Functions
- [ ] OR ???
- [ ] Join ???

### General

- ✅ Partition Pruning
- ✅ Projection pushdown
- ✅ Predicate push down ([Value range](https://cloud.google.com/bigtable/docs/using-filters#value-range))
- [ ] Limit Pushdown ???

## Limitation

- for row key encoding, only string is supported
- for single/composite row key, identifiers supports "=" and "IN". Additionally, last identifier also supports "BETWEEN".
- for qualifiers, only string and integer (64bit BigEndian encoding) value are supported
- subqueries and common table expressions are not supported

## Technical Details

BigtableQL depends on
- [sqloxide](https://github.com/wseaton/sqloxide) and [sqlparser-rs](https://github.com/sqlparser-rs/sqlparser-rs): SQL parser
- [python-bigtable](https://github.com/googleapis/python-bigtable): offical python bigtable client
- [datafusion-python](https://github.com/datafusion-contrib/datafusion-python): in memory query engine
- [pyarrow](https://github.com/apache/arrow/tree/master/python): in memory columnar store / dataframe
