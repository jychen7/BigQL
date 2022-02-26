def parse(insert):
    table_name = insert["table_name"][0]["value"]

    keys = [col["value"] for col in insert["columns"]]

    values_batch = [
        [_parse_value(element) for element in values]
        for values in insert["source"]["body"]["Values"]
    ]
    return table_name, keys, values_batch


def _parse_value(element: dict):
    if "Identifier" in element:
        return element["Identifier"]["value"]

    value = (
        element.get("Value", {}).get("SingleQuotedString")
        or element.get("Value", {}).get("Number", (None,))[0]
    )
    if not value:
        raise Exception("insert: values invalid, should be string or integer")
    return value
