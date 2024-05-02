from ddlsparkconverter.main_converter import DatabaseConverter

ddl = """
CREATE TABLE test.Employees (
    EmployeeID INT,
    FirstName VARCHAR,
    LastName VARCHAR,
    Department VARCHAR,
    Salary DECIMAL(10, 2),
    Salary2 REAL,
);
"""

d = DatabaseConverter(source_db="mssql", target_db="oracle", ddl_text=ddl)


print(d.run())


# result = DDLParser(ddl).run(json_dump=True, output_mode="oracle")
# print(result)
