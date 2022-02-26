import pytest
from google.auth.credentials import AnonymousCredentials
from bigql.client import Client
from bigql.select import SelectQuery


@pytest.fixture
def catalog():
    return {
        "users": {
            "table_name": "users",
            "instance_id": "my_instance",
            "column_families": {
                "profile": {
                    "only_read_latest": True,
                    "columns": {"age": int, "gender": str},
                },
            },
            "row_key_identifiers": ["_row_key"],
            "row_key_separator": "#",
        },
        "balloons": {
            "table_name": "balloons",
            "instance_id": "my_instance",
            "column_families": {
                "measurements": {
                    "only_read_latest": True,
                    "columns": {"pressure": int, "temperature": str},
                },
            },
            "row_key_identifiers": ["device_id", "event_minute"],
            "row_key_separator": "#",
        },
    }


@pytest.fixture
def anonymous_client():
    return Client(project="my_project", credentials=AnonymousCredentials())
