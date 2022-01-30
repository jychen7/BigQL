from bigtableql.select import validator
import pytest


def test_select_no_table(catalog):
    with pytest.raises(Exception) as e:
        validator.validate_table_name(catalog, "customers")
    assert str(e.value) == "catalog: customers not found, please register_table first"


def test_select_no_column_family(catalog):
    with pytest.raises(Exception) as e:
        validator.validate_column_family("users", catalog["users"], "friends")
    assert str(e.value) == "table users: column_family friends not found"


def test_select_no_column(catalog):
    with pytest.raises(Exception) as e:
        validator.validate_columns("users", catalog["users"], "profile", ["address"])
    assert str(e.value) == "table users, column_family profile: address not found"
