def validate_table_name(catalog, table_name):
    if table_name not in catalog:
        raise Exception(f"catalog: {table_name} not found, please register_table first")


def validate_column_family(table_name, table_catalog, column_family_id):
    if column_family_id not in table_catalog["column_families"]:
        raise Exception(
            f"table {table_name}: column_family {column_family_id} not found"
        )


def validate_columns(table_name, table_catalog, column_family_id, qualifiers):
    columns = set(table_catalog["column_families"][column_family_id]["columns"].keys())

    for qualifier in qualifiers:
        if qualifier not in columns:
            raise Exception(
                f"table {table_name}, column_family {column_family_id}: {qualifier} not found"
            )
