import pytest


def test_register_table(anonymous_client, catalog):
    anonymous_client.register_table(
        "users",
        instance_id="my_instance",
        column_families={
            "profile": {
                "only_read_latest": True,
                "columns": {"age": int, "gender": str},
            }
        },
    )
    assert anonymous_client.catalog["users"] == catalog["users"]
