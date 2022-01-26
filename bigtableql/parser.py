import sqloxide
import functools
from typing import Tuple

BINARY_OPERATION = "BinaryOp"
FUNCTION_OPERATION = "Function"

AND_OPERATOR = "And"
EQUAL_OPERATOR = "Eq"
IN_OPERATOR = "InList"
BETWEEN_OPERATOR = "Between"


def parse(sql: str, catalog: dict) -> Tuple[str, set, set, dict]:
    parsed = sqloxide.parse_sql(sql=sql, dialect="ansi")

    select = parsed[0]["Query"]["body"]["Select"]
    table_name = _parse_table_name(select["from"])

    if table_name not in catalog:
        raise Exception(f"catalog: {table_name} not found, please register_table first")
    table_catalog = catalog[table_name]

    projection = _parse_projection(select["projection"])
    if select["selection"]:
        selection = _parse_selection(select["selection"])
        row_key_identifiers_mapping = _parse_identifier_mapping(
            table_catalog["row_key_identifiers"], select["selection"]
        )
    else:
        selection = set()
        row_key_identifiers_mapping = {}

    return table_name, projection, selection, row_key_identifiers_mapping


def _parse_table_name(select_from) -> str:
    return select_from[0]["relation"]["Table"]["name"][0]["value"]


def _parse_projection(select_projection) -> set:
    projection = [
        [
            _parse_identifier_key(arg["Unnamed"])
            for arg in expr["UnnamedExpr"][FUNCTION_OPERATION]["args"]
        ]
        if FUNCTION_OPERATION in expr["UnnamedExpr"]
        else _parse_identifier_key(expr["UnnamedExpr"])
        for expr in select_projection
    ]
    return functools.reduce(
        lambda sum, x: sum | set(x) if isinstance(x, list) else sum | set([x]),
        projection,
        set(),
    )


def _parse_selection(select_selection) -> set:
    if IN_OPERATOR in select_selection:
        identifier = _parse_identifier_key(select_selection[IN_OPERATOR]["expr"])
        return {identifier}

    if BETWEEN_OPERATOR in select_selection:
        identifier = _parse_identifier_key(select_selection[BETWEEN_OPERATOR]["expr"])
        return {identifier}

    if BINARY_OPERATION not in select_selection:
        raise Exception(
            f"selection: only {IN_OPERATOR}, {BETWEEN_OPERATOR} and {BINARY_OPERATION} are supported"
        )

    left = select_selection[BINARY_OPERATION]["left"]
    op = select_selection[BINARY_OPERATION]["op"]
    right = select_selection[BINARY_OPERATION]["right"]
    if op == "And":
        return _parse_selection(left) | _parse_selection(right)

    identifier = _parse_identifier_key(select_selection[BINARY_OPERATION]["left"])
    return {identifier}


def _parse_identifier_key(left):
    return left["Identifier"]["value"]


def _parse_identifier_value(right):
    if "Identifier" in right:
        # {'Identifier': {'value': 'a', 'quote_style': '"'}}
        return right["Identifier"]["value"]

    # {'Value': {'SingleQuotedString': '20220116'}}
    return right.get("Value", {}).get("SingleQuotedString", {})


def _parse_identifier_mapping(row_key_identifiers, select_selection) -> dict:
    if IN_OPERATOR in select_selection:
        identifier = _parse_identifier_key(select_selection[IN_OPERATOR]["expr"])
        values = [
            _parse_identifier_value(v) for v in select_selection[IN_OPERATOR]["list"]
        ]
        return {identifier: values}

    if BETWEEN_OPERATOR in select_selection:
        identifier = _parse_identifier_key(select_selection[BETWEEN_OPERATOR]["expr"])
        low = _parse_identifier_value(select_selection[BETWEEN_OPERATOR]["low"])
        high = _parse_identifier_value(select_selection[BETWEEN_OPERATOR]["high"])
        return {identifier: (low, high)}

    if BINARY_OPERATION not in select_selection:
        raise Exception(
            f"selection: only {IN_OPERATOR}, {BETWEEN_OPERATOR} and {BINARY_OPERATION} are supported"
        )

    left = select_selection[BINARY_OPERATION]["left"]
    op = select_selection[BINARY_OPERATION]["op"]
    right = select_selection[BINARY_OPERATION]["right"]
    if op == AND_OPERATOR:
        return _merge_no_duplicate(
            _parse_identifier_mapping(row_key_identifiers, left),
            _parse_identifier_mapping(row_key_identifiers, right),
        )

    identifier = _parse_identifier_key(left)
    if identifier not in row_key_identifiers:
        return {}

    if op == EQUAL_OPERATOR:
        value = _parse_identifier_value(right)
        return {identifier: [value]}
    else:
        raise Exception(
            f"selection ({identifier}): only {IN_OPERATOR}, {BETWEEN_OPERATOR} and {BINARY_OPERATION} are supported"
        )


def _merge_no_duplicate(identifier_map1, identifier_map2):
    for identifier in identifier_map1.keys():
        if identifier in identifier_map2:
            raise Exception(f"selection: {identifier} is duplicated")
    identifier_map1.update(identifier_map2)
    return identifier_map1
