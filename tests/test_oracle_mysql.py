from ddlsparkconverter.main_converter import DatabaseConverter
import pytest

SOURCE_DB = "oracle"
TARGET_DB = "mysql"

test_ddls = [
    """
        CREATE TABLE test.employees (
            employee_id NUMBER(5),
            first_name VARCHAR,
            hire_date DATE,
            salary NUMBER(10, 2)
        );
    """
]

expected_ddls = [
"""CREATE TABLE test.employees (
 employee_id DECIMAL(5, 0), 
 first_name LONGTEXT, 
 hire_date TIMESTAMP(6), 
 salary DECIMAL(38, 10), 
);"""
]


@pytest.mark.parametrize("test_ddl,expected_ddl", [(test_ddls[0], expected_ddls[0])])
def test_simple_create(test_ddl, expected_ddl):
    d = DatabaseConverter(source_db=SOURCE_DB, target_db=TARGET_DB, ddl_text=test_ddl)

    result_ddl = d.run()

    assert result_ddl.replace("\t", " ") == expected_ddl
