from bigtableql.select import parser
import pytest
import sqloxide
import yaml


def test_parse(catalog):
    with open("tests/select/sql.yaml", "r") as stream:
        test_cases = yaml.load(stream, yaml.FullLoader)
    for test in test_cases:
        sql = test["sql"]
        parsed = sqloxide.parse_sql(sql=sql, dialect="ansi")[0]
        select = parsed["Query"]["body"]["Select"]

        if "parse_error" in test:
            with pytest.raises(Exception) as e:
                parser.parse(select, catalog)
            assert str(e.value) == test["parse_error"]
        else:
            table_name, projection, selection, row_key_identifiers_mapping = test[
                "parse_success"
            ]
            assert parser.parse(select, catalog) == (
                table_name,
                set(projection),
                set(selection),
                row_key_identifiers_mapping,
            )
