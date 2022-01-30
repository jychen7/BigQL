from bigtableql.insert import composer


def test_compose_simple_row_key(catalog):
    rows = composer.compose(
        catalog["users"],
        "profile",
        ["_row_key", "gender", "age"],
        [["abc", "m", 10], ["def", "f", 10]],
    )
    assert len(rows) == 2
    assert rows[0].row_key == b"abc"
    assert rows[1].row_key == b"def"


def test_compose_composite_row_key(catalog):
    rows = composer.compose(
        catalog["balloons"],
        "measurements",
        ["device_id", "event_minute", "pressure", "temperature"],
        [
            ["3698", "2021-03-05-1200", 123, "37.1"],
            ["3700", "2021-03-05-1201", 234, "37.2"],
        ],
    )
    assert len(rows) == 2
    assert rows[0].row_key == b"3698#2021-03-05-1200"
    assert rows[1].row_key == b"3700#2021-03-05-1201"
