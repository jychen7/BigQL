from bigql.insert import parser
import pytest
import sqloxide
import yaml


def test_parse(catalog):
    with open("tests/insert/sql.yaml", "r") as stream:
        test_cases = yaml.load(stream, yaml.FullLoader)
    for test in test_cases:
        sql = test["sql"]
        parsed = sqloxide.parse_sql(sql=sql, dialect="ansi")[0]
        insert = parsed["Insert"]

        if "parse_error" in test:
            with pytest.raises(Exception) as e:
                parser.parse(insert)
            assert str(e.value) == test["parse_error"]
        else:
            table_name, keys, values_batch = test["parse_success"]
            assert parser.parse(insert) == (table_name, keys, values_batch)
