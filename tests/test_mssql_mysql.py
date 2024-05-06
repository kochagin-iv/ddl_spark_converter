import pytest

from ddl_spark_converter.main_converter import DatabaseConverter

SOURCE_DB = "mssql"
TARGET_DB = "mysql"

test_ddls = open("tests/input_ddl/mssql_mysql/simple_create.txt", "r")
expected_ddls = open("tests/output_ddl/mssql_mysql/simple_create.txt", "r")

test_ddls = [test_ddl for test_ddl in test_ddls.read().split("\n\n")]
expected_ddls = [test_ddl for test_ddl in expected_ddls.read().split("\n\n")]

tests = [
    (test_ddl, expected_ddl) for test_ddl, expected_ddl in zip(test_ddls, expected_ddls)
]


@pytest.mark.parametrize("test_ddl,expected_ddl", tests)
def test_simple_create(test_ddl, expected_ddl):
    d = DatabaseConverter(source_db=SOURCE_DB, target_db=TARGET_DB, ddl_text=test_ddl)

    result_ddl = d.run()

    assert result_ddl == expected_ddl
