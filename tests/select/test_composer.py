from bigtableql.select import composer, scanner
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
                row_set, predicate_filters = composer.compose(
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

                if "compose_predicates" not in test:
                    assert predicate_filters == []
                else:
                    first_predicate = test["compose_predicates"][0]
                    is_int = isinstance(first_predicate[0] or first_predicate[1], int)
                    assert [
                        (
                            scanner._decode_cell_value(f.start_value, is_int),
                            scanner._decode_cell_value(f.end_value, is_int),
                            f.inclusive_start,
                            f.inclusive_end,
                        )
                        for f in predicate_filters
                    ] == test["compose_predicates"]
