from bigql.insert import validator
import pytest


def test_not_register(catalog):
    with pytest.raises(Exception) as e:
        validator.validate_columns(
            catalog["users"],
            "profile",
            keys=["address"],
            values_batch=[],
        )
    assert str(e.value) == "insert: {'address'} not registered"


def test_row_identifiers_required(catalog):
    with pytest.raises(Exception) as e:
        validator.validate_columns(
            catalog["balloons"],
            "measurements",
            keys=["device_id"],
            values_batch=[["3698"]],
        )
    assert str(e.value) == "insert: {'event_minute'} required"


def test_length_match(catalog):
    with pytest.raises(Exception) as e:
        validator.validate_columns(
            catalog["users"],
            "profile",
            keys=["_row_key", "age"],
            values_batch=[["abc", 1], ["def", 2, 3]],
        )
    assert str(e.value) == "insert: ['def', 2, 3] is invalid, should have 2 elements"
