import functools
from typing import Tuple
from bigql import SELECT_STAR
from bigql.select import validator

BINARY_OPERATION = "BinaryOp"
FUNCTION_OPERATION = "Function"

AND_OPERATOR = "And"
EQUAL_OPERATOR = "Eq"
GREATER_THAN_OPERATOR = "Gt"
LESS_THAN_OPERATOR = "Lt"
GREATER_OR_EQUAL_OPERATOR = "GtEq"
LESS_OR_EQUAL_OPERATOR = "LtEq"

IN_OPERATOR = "InList"
BETWEEN_OPERATOR = "Between"


def parse(select, catalog: dict) -> Tuple[str, set, set, dict]:
    table_name = _parse_table_name(select["from"])

    validator.validate_table_name(catalog, table_name)
    table_catalog = catalog[table_name]

    projection = _parse_projection(select["projection"])
    if select["selection"]:
        selection = _parse_selection(select["selection"])
        identifiers_mapping = _parse_identifier_mapping(
            table_catalog["row_key_identifiers"], select["selection"]
        )
    else:
        selection = set()
        identifiers_mapping = {}

    return table_name, projection, selection, identifiers_mapping


def _parse_table_name(select_from) -> str:
    return select_from[0]["relation"]["Table"]["name"][0]["value"]


def _parse_projection(select_projection: list) -> set:
    if SELECT_STAR in select_projection:
        return {SELECT_STAR}

    projection = []
    for expr in select_projection:
        if "ExprWithAlias" in expr:
            projection.append(_parse_identifier_key(expr["ExprWithAlias"]["expr"]))
            continue

        projection.append(_parse_identifier_key(expr["UnnamedExpr"]))

    return set(projection)


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


def _parse_identifier_key(expr):
    if FUNCTION_OPERATION in expr:
        expr = expr[FUNCTION_OPERATION]["args"][0]["Unnamed"]

    if "Cast" in expr:
        expr = expr["Cast"]["expr"]

    if "Identifier" in expr:
        return expr["Identifier"]["value"]

    if BINARY_OPERATION in expr:
        return _parse_identifier_key(
            expr[BINARY_OPERATION]["left"]
        ) or _parse_identifier_key(expr[BINARY_OPERATION]["right"])
    return None


def _parse_identifier_value(row_key_identifiers, identifier, expr):
    if "Identifier" in expr:
        # {'Identifier': {'value': 'a', 'quote_style': '"'}}
        return expr["Identifier"]["value"]

    # {'Value': {'SingleQuotedString': '20220116'}}
    # {'Value': {'Number': ('1', False)}}
    # {'Value': {'Number': ('1.1', False)}}
    value = expr.get("Value", {})
    if not value:
        raise Exception(f"selection ({identifier}): unable to parse value")

    if "SingleQuotedString" in value:
        return value["SingleQuotedString"]

    if identifier in row_key_identifiers:
        raise Exception(
            f"selection ({identifier}): row identifier only support string value"
        )

    number = value["Number"][0]
    try:
        return int(number)
    except:
        raise Exception(
            f"selection ({identifier}): qualifier only support string and integer value"
        )


def _parse_identifier_mapping(row_key_identifiers, select_selection) -> dict:
    if IN_OPERATOR in select_selection:
        identifier = _parse_identifier_key(select_selection[IN_OPERATOR]["expr"])
        values = [
            _parse_identifier_value(row_key_identifiers, identifier, v)
            for v in select_selection[IN_OPERATOR]["list"]
        ]
        return {identifier: values}

    if BETWEEN_OPERATOR in select_selection:
        identifier = _parse_identifier_key(select_selection[BETWEEN_OPERATOR]["expr"])
        low = _parse_identifier_value(
            row_key_identifiers, identifier, select_selection[BETWEEN_OPERATOR]["low"]
        )
        high = _parse_identifier_value(
            row_key_identifiers, identifier, select_selection[BETWEEN_OPERATOR]["high"]
        )
        return {identifier: (low, high, True, True)}

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
    if op == EQUAL_OPERATOR:
        value = _parse_identifier_value(row_key_identifiers, identifier, right)
        return {identifier: [value]}
    elif identifier in row_key_identifiers:
        raise Exception(
            f"selection ({identifier}): only {IN_OPERATOR}, {BETWEEN_OPERATOR} and {BINARY_OPERATION} are supported"
        )

    value = _parse_identifier_value(row_key_identifiers, identifier, right)
    if op == GREATER_OR_EQUAL_OPERATOR:
        return {identifier: (value, None, True, None)}
    elif op == GREATER_THAN_OPERATOR:
        return {identifier: (value, None, False, None)}
    elif op == LESS_OR_EQUAL_OPERATOR:
        return {identifier: (None, value, None, True)}
    elif op == LESS_THAN_OPERATOR:
        return {identifier: (None, value, None, False)}
    else:
        raise Exception(f"selection ({identifier}): {op} not supported")


def _merge_no_duplicate(identifier_map1, identifier_map2):
    for identifier in identifier_map1.keys():
        if identifier in identifier_map2:
            raise Exception(f"selection: {identifier} is duplicated")
    identifier_map1.update(identifier_map2)
    return identifier_map1
