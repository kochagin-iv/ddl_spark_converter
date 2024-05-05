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

d = DatabaseConverter(source_db="mssql", target_db="oracle", ddl_text=ddl)


print(d.run())


# result = DDLParser(ddl).run(json_dump=True, output_mode="oracle")
# print(result)
