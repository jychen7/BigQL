from bigtableql import scanner
import struct
from google.cloud.bigtable.row_data import PartialRowData, Cell
from datetime import datetime


def test_process_row_simple_key_single_qualifier_single_cell():
    columnar = {"_row_key": [], "_timestamp": [], "age": []}
    row_data = PartialRowData(b"abc#def")
    row_data._cells["profile"] = {}
    row_data._cells["profile"][b"age"] = [
        Cell(struct.Struct(">q").pack(20), 1641013200000000)
    ]
    scanner._process_row(
        columnar, row_data, ["_row_key"], "#", "profile", set(["age"]), set(["age"])
    )
    assert columnar["_row_key"] == ["abc#def"]
    assert columnar["_timestamp"] == ["2022-01-01 05:00:00+00:00"]
    assert columnar["age"] == [20]


def test_process_row_simple_key_single_qualifier_multiple_cells():
    columnar = {"_row_key": [], "_timestamp": [], "age": []}
    row_data = PartialRowData(b"abc")
    row_data._cells["profile"] = {}
    row_data._cells["profile"][b"age"] = [
        Cell(struct.Struct(">q").pack(20), 1641013200000000),
        Cell(struct.Struct(">q").pack(10), 1641013100000000),
    ]
    scanner._process_row(
        columnar, row_data, ["_row_key"], "#", "profile", set(["age"]), set(["age"])
    )
    assert columnar["_row_key"] == ["abc", "abc"]
    assert columnar["_timestamp"] == [
        "2022-01-01 05:00:00+00:00",
        "2022-01-01 04:58:20+00:00",
    ]
    assert columnar["age"] == [20, 10]


def test_process_row_simple_key_multiple_qualifiers_single_cell():
    columnar = {"_row_key": [], "_timestamp": [], "age": [], "gender": []}
    row_data = PartialRowData(b"abc")
    row_data._cells["profile"] = {}
    row_data._cells["profile"][b"age"] = [
        Cell(struct.Struct(">q").pack(20), 1641013200000000),
    ]
    row_data._cells["profile"][b"gender"] = [
        Cell(b"M", 1641013200000000),
    ]
    scanner._process_row(
        columnar,
        row_data,
        ["_row_key"],
        "#",
        "profile",
        set(["age", "gender"]),
        set(["age"]),
    )
    assert columnar["_row_key"] == ["abc"]
    assert columnar["_timestamp"] == ["2022-01-01 05:00:00+00:00"]
    assert columnar["age"] == [20]
    assert columnar["gender"] == ["M"]


def test_process_row_simple_key_multiple_qualifiers_multiple_cells():
    columnar = {"_row_key": [], "_timestamp": [], "age": [], "gender": []}
    row_data = PartialRowData(b"abc")
    row_data._cells["profile"] = {}
    row_data._cells["profile"][b"age"] = [
        Cell(struct.Struct(">q").pack(20), 1641013200000000),
        Cell(struct.Struct(">q").pack(10), 1641013100000000),
    ]
    row_data._cells["profile"][b"gender"] = [
        Cell(b"M", 1641013200000000),
        Cell(b"M", 1641013100000000),
    ]
    scanner._process_row(
        columnar,
        row_data,
        ["_row_key"],
        "#",
        "profile",
        set(["age", "gender"]),
        set(["age"]),
    )
    assert columnar["_row_key"] == ["abc", "abc"]
    assert columnar["_timestamp"] == [
        "2022-01-01 05:00:00+00:00",
        "2022-01-01 04:58:20+00:00",
    ]
    assert columnar["age"] == [20, 10]
    assert columnar["gender"] == ["M", "M"]


def test_process_row_composite_key():
    columnar = {
        "device_id": [],
        "event_minute": [],
        "_timestamp": [],
        "temperature": [],
        "pressure": [],
    }
    row_data = PartialRowData(b"3698#2022-01-01-0500")
    row_data._cells["measurements"] = {}
    row_data._cells["measurements"][b"temperature"] = [
        Cell(b"37.1", 1641013200000000),
        Cell(b"36.9", 1641013100000000),
    ]
    row_data._cells["measurements"][b"pressure"] = [
        Cell(struct.Struct(">q").pack(94558), 1641013200000000),
        Cell(struct.Struct(">q").pack(94122), 1641013100000000),
    ]
    scanner._process_row(
        columnar,
        row_data,
        ["device_id", "event_minute"],
        "#",
        "measurements",
        set(["temperature", "pressure"]),
        set(["pressure"]),
    )
    assert columnar["device_id"] == ["3698", "3698"]
    assert columnar["event_minute"] == ["2022-01-01-0500", "2022-01-01-0500"]
    assert columnar["_timestamp"] == [
        "2022-01-01 05:00:00+00:00",
        "2022-01-01 04:58:20+00:00",
    ]
    assert columnar["temperature"] == ["37.1", "36.9"]
    assert columnar["pressure"] == [94558, 94122]


def test_int_qualifiers():
    columns_map = {
        "age": int,
        "gender": str,
    }
    assert scanner._int_qualifiers(set(["age"]), columns_map) == set(["age"])
    assert scanner._int_qualifiers(set(["gender"]), columns_map) == set([])
    assert scanner._int_qualifiers(set(["age"]), {}) == set([])


def test_row_chain():
    assert len(scanner._row_chain("profile", {}, set())) == 2
    assert len(scanner._row_chain("profile", {"only_read_latest": True}, set())) == 3


def test_decode_cell_value_str():
    assert scanner._decode_cell_value(b"123", is_int=False) == "123"


def test_decode_cell_value_int():
    a = 123
    assert scanner._decode_cell_value(struct.Struct(">q").pack(123), is_int=True) == a
