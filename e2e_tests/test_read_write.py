import os

from google.auth.credentials import AnonymousCredentials
from bigtableql.client import Client

PROJECT_ID = "my_project"
INSTANCE_ID = "my_instance"
TABLE_NAME = "weather_balloons"
COLUMN_FAMILY_ID = "measurements"


def test_read_after_write():
    os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
    # set admin=True to create table
    client = Client(project=PROJECT_ID, credentials=AnonymousCredentials(), admin=True)
    bigtable_table = client.bigtable_client.instance(INSTANCE_ID).table(TABLE_NAME)

    _prepare_table(bigtable_table)
    _prepare_column_family(bigtable_table)
    _registry_table_simple(client)
    _test_read_empty(client)

    _test_write(client)
    _test_read_simple(client)
    _test_read_simple_predicate(client)

    _registry_table_composite(client)
    _test_read_composite(client)


def _prepare_table(bigtable_table):
    try:
        bigtable_table.create()
    except:
        pass


def _prepare_column_family(bigtable_table):
    try:
        bigtable_table.column_family(COLUMN_FAMILY_ID).create()
    except:
        pass


def _registry_table_simple(client):
    client.register_table(
        TABLE_NAME,
        instance_id=INSTANCE_ID,
        column_families={
            COLUMN_FAMILY_ID: {
                "only_read_latest": True,
                "columns": {
                    "pressure": int,
                    "temperature": str,
                    "humidity": int,
                    "altitude": int,
                },
            }
        },
    )


def _registry_table_composite(client):
    client.register_table(
        TABLE_NAME,
        instance_id=INSTANCE_ID,
        column_families={
            COLUMN_FAMILY_ID: {
                "only_read_latest": True,
                "columns": {
                    "pressure": int,
                    "temperature": str,
                    "humidity": int,
                    "altitude": int,
                },
            }
        },
        row_key_identifiers=["location", "balloon_id", "event_minute"],
        row_key_separator="#",
    )


def _test_write(client):
    responses = client.query(
        "measurements",
        """
        INSERT INTO weather_balloons
        ("_row_key", "pressure", "temperature")
        values
        ("us-west2#3698#2021-03-05-1200", 94558, "76"),
        ("us-west2#3698#2021-03-05-1201", 94122, "78")
    """,
    )
    assert responses[0][0]
    assert responses[1][0]


def _test_read_empty(client):
    record_batchs = client.query(
        "measurements",
        """
        SELECT avg(pressure) as avg_pressure FROM weather_balloons
        WHERE
        "_row_key" BETWEEN 'us-west2#3698#2021-03-05-1200' AND 'us-west2#3698#2021-03-05-1204'
    """,
    )
    assert len(record_batchs) == 1
    assert record_batchs[0].num_rows == 0


def _test_read_simple(client):
    record_batchs = client.query(
        "measurements",
        """
        SELECT avg(pressure) as avg_pressure FROM weather_balloons
        WHERE
        "_row_key" BETWEEN 'us-west2#3698#2021-03-05-1200' AND 'us-west2#3698#2021-03-05-1204'
    """,
    )
    assert record_batchs[0].to_pydict() == {"avg_pressure": [94340.0]}


def _test_read_simple_predicate(client):
    record_batchs = client.query(
        "measurements",
        """
        SELECT avg(pressure) as avg_pressure FROM weather_balloons
        WHERE
        "_row_key" BETWEEN 'us-west2#3698#2021-03-05-1200' AND 'us-west2#3698#2021-03-05-1204'
        AND "pressure" >= 94558
    """,
    )
    assert record_batchs[0].to_pydict() == {"avg_pressure": [94558.0]}


def _test_read_composite(client):
    record_batchs = client.query(
        "measurements",
        """
        SELECT avg(cast(temperature as int)) as avg_temperature FROM weather_balloons
        WHERE
          location = 'us-west2'
          AND balloon_id = '3698'
          AND event_minute BETWEEN '2021-03-05-1200' AND '2021-03-05-1204'
            """,
    )
    assert record_batchs[0].to_pydict() == {"avg_temperature": [77]}
