from bigtableql.select import SelectQuery
import sqloxide
import pytest


def test_select_no_table(anonymous_client, catalog):
    column_family_id = "profile"
    sql = """select age from customers where "_row_key" = 'abc'"""

    with pytest.raises(Exception) as e:
        parsed_select = sqloxide.parse_sql(sql=sql, dialect="ansi")[0]["Query"]["body"][
            "Select"
        ]
        SelectQuery(
            anonymous_client.bigtable_client,
            catalog,
            column_family_id,
            parsed_select,
            sql,
        ).execute()
    assert str(e.value) == "catalog: customers not found, please register_table first"


def test_select_no_column_family(anonymous_client, catalog):
    column_family_id = "friends"
    sql = """select age from users where "_row_key" = 'abc'"""

    with pytest.raises(Exception) as e:
        parsed_select = sqloxide.parse_sql(sql=sql, dialect="ansi")[0]["Query"]["body"][
            "Select"
        ]
        SelectQuery(
            anonymous_client.bigtable_client,
            catalog,
            column_family_id,
            parsed_select,
            sql,
        ).execute()
    assert str(e.value) == "table users: column_family friends not found"


def test_select_no_column(anonymous_client, catalog):
    column_family_id = "profile"
    sql = """select address from users where "_row_key" = 'abc'"""

    with pytest.raises(Exception) as e:
        parsed_select = sqloxide.parse_sql(sql=sql, dialect="ansi")[0]["Query"]["body"][
            "Select"
        ]
        SelectQuery(
            anonymous_client.bigtable_client,
            catalog,
            column_family_id,
            parsed_select,
            sql,
        ).execute()
    assert str(e.value) == "table users, column_family profile: address not found"
