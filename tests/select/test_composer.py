from bigtableql.select import composer
import yaml
import pytest


def test_composer(catalog):
    with open("tests/select/sql.yaml", "r") as stream:
        test_cases = yaml.load(stream, yaml.FullLoader)
    for test in test_cases:
        if "parse_success" in test:
            table_name, _, _, row_key_identifiers_mapping = test["parse_success"]
            if "compose_error" in test:
                with pytest.raises(Exception) as e:
                    composer.compose(catalog[table_name], row_key_identifiers_mapping)
                assert str(e.value) == test["compose_error"]
            else:
                row_set = composer.compose(
                    catalog[table_name], row_key_identifiers_mapping
                )
                output = test["compose_success"]
                if isinstance(output[0], tuple):
                    assert [
                        (
                            r.start_key.decode(),
                            r.end_key.decode(),
                            r.start_inclusive,
                            r.end_inclusive,
                        )
                        for r in row_set.row_ranges
                    ] == output
                else:
                    assert row_set.row_keys == output
