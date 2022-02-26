def validate_columns(table_catalog, column_family_id, keys, values_batch):
    columns = table_catalog["column_families"][column_family_id]["columns"].keys()

    row_key_identifiers = table_catalog["row_key_identifiers"]
    unregisterd_keys = set(keys) - (set(row_key_identifiers) | set(columns))
    if unregisterd_keys:
        raise Exception(f"insert: {unregisterd_keys} not registered")

    missing_identifiers = set(row_key_identifiers) - set(keys)
    if missing_identifiers:
        raise Exception(f"insert: {missing_identifiers} required")

    n = len(keys)
    for values in values_batch:
        if len(values) != n:
            raise Exception(f"insert: {values} is invalid, should have {n} elements")
