from bigtableql import parser
import yaml
import pytest


def test_parse(catalog):
    with open("tests/sql.yaml", "r") as stream:
        test_cases = yaml.load(stream, yaml.FullLoader)
    for test in test_cases:
        if "parse_error" in test:
            with pytest.raises(Exception) as e:
                parser.parse(test["sql"], catalog)
            assert str(e.value) == test["parse_error"]
        else:
            table_name, projection, selection, row_key_identifiers_mapping = test[
                "parse_success"
            ]
            assert parser.parse(test["sql"], catalog) == (
                table_name,
                set(projection),
                set(selection),
                row_key_identifiers_mapping,
            )
