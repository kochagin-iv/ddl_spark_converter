from ddl_spark_converter.main_converter import DatabaseConverter

ddl = """
CREATE TABLE IF NOT EXISTS test.Persons (
    PersonID FLOAT,
    LastName varchar,
    Money DECIMAL(5),
    time TIME,
    xml XML,
);
"""

converter = DatabaseConverter(source_db="mssql", target_db="oracle", ddl_text=ddl)

result_ddl = converter.run()

expected_ddl = """CREATE TABLE IF NOT EXISTS test.Persons (
    PersonID NUMBER(19, 4),
    LastName CLOB,
    Money NUMBER(5, 0),
    time TIMESTAMP(6),
    xml CLOB,
);"""

assert result_ddl == expected_ddl
