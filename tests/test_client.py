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


def test_select_no_table(anonymous_client, catalog):
    anonymous_client.catalog = catalog
    with pytest.raises(Exception) as e:
        anonymous_client.query(
            "profile", """select age from customers where "_row_key" = 'abc'"""
        )
    assert str(e.value) == "catalog: customers not found, please register_table first"


def test_select_no_column_family(anonymous_client, catalog):
    anonymous_client.catalog = catalog
    with pytest.raises(Exception) as e:
        anonymous_client.query(
            "friends", """select age from users where "_row_key" = 'abc'"""
        )
    assert str(e.value) == "table users: column_family friends not found"


def test_select_no_column(anonymous_client, catalog):
    anonymous_client.catalog = catalog
    with pytest.raises(Exception) as e:
        anonymous_client.query(
            "profile", """select address from users where "_row_key" = 'abc'"""
        )
    assert str(e.value) == "table users, column_family profile: address not found"
