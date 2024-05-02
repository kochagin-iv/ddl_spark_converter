from ddlsparkconverter.db_from_spark_datatype_conversion_info.mssql import (
    datatypes as spark_mssql_datatypes,
)
from ddlsparkconverter.db_to_spark_datatype_conversion_info.mssql import (
    datatypes as mssql_spark_datatypes,
)

from ddlsparkconverter.db_converter.IConverter import IConverter
from ddlsparkconverter.db_converter.utils import generate_full_type_name


class MSSQLConverter(IConverter):
    def __init__(self):
        self.to_spark_converter = mssql_spark_datatypes
        self.to_mssql_converter = spark_mssql_datatypes

    def convert_to_spark_ddl(self, ddl_text):
        spark_table_info = ddl_text[0]

        for i, column in enumerate(spark_table_info["columns"]):

            column_full_type_name = generate_full_type_name(column=column)

            spark_table_info["columns"][i]["full_type_name"] = self.to_spark_converter[
                column_full_type_name
            ]
        return spark_table_info

    def convert_from_spark_ddl(self, spark_table_info):

        mssql_table_info = spark_table_info

        mssql_table_name = mssql_table_info["table_name"]

        if mssql_table_info.get("schema", ""):
            mssql_table_name = f"{mssql_table_info['schema']}.{mssql_table_name}"

        mssql_text_ddl = f"CREATE TABLE {mssql_table_name} (\n"

        for column in mssql_table_info["columns"]:

            column_full_name = column["full_type_name"]

            column_full_type_name = self.to_mssql_converter[column_full_name]

            mssql_text_ddl += f"\t{column['name']} {column_full_type_name}, \n"

        mssql_text_ddl += ");"

        return mssql_text_ddl
