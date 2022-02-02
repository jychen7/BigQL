import os

from google.auth.credentials import AnonymousCredentials
from bigtableql.client import Client

project_id = "my_project"
instance_id = "my_instance"
table_name = "weather_balloons"
column_family_id = "measurements"


def test_read_after_write():
    os.environ["BIGTABLE_EMULATOR_HOST"] = "localhost:8086"
    # set admin=True to create table
    client = Client(project=project_id, credentials=AnonymousCredentials(), admin=True)

    bigtable_table = client.bigtable_client.instance(instance_id).table(table_name)
    try:
        bigtable_table.create()
    except:
        pass

    try:
        bigtable_table.column_family(column_family_id).create()
    except:
        pass

    client.register_table(
        table_name,
        instance_id=instance_id,
        column_families={
            column_family_id: {
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

    # write
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

    # read via row_key
    record_batchs = client.query(
        "measurements",
        """
        SELECT avg(pressure) as avg_pressure FROM weather_balloons
        WHERE
        "_row_key" BETWEEN 'us-west2#3698#2021-03-05-1200' AND 'us-west2#3698#2021-03-05-1204'
    """,
    )
    assert record_batchs[0].to_pydict() == {"avg_pressure": [94340.0]}

    # read via composite key
    client.register_table(
        table_name,
        instance_id=instance_id,
        column_families={
            column_family_id: {
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
