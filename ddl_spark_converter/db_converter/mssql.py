from ddl_spark_converter.db_converter.IConverter import IConverter
from ddl_spark_converter.db_converter.utils import generate_full_type_name
from ddl_spark_converter.db_from_spark_datatype_conversion_info.mssql import (
    datatypes as spark_mssql_datatypes,
)
from ddl_spark_converter.db_to_spark_datatype_conversion_info.mssql import (
    datatypes as mssql_spark_datatypes,
)


class MSSQLConverter(IConverter):
    def __init__(self):
        self.to_spark_converter = mssql_spark_datatypes
        self.to_mssql_converter = spark_mssql_datatypes

    def convert_to_spark_ddl(self, ddl_text):
        spark_table_info = ddl_text

        for i, column in enumerate(spark_table_info["columns"]):

            mssql_full_type_name = generate_full_type_name(column=column)
            spark_full_type_name = self.to_spark_converter.get(mssql_full_type_name)

            if not spark_full_type_name:
                raise Exception(
                    f"MSSQL datatype {mssql_full_type_name} cannot be converted to Spark datatypes"
                )

            spark_table_info["columns"][i]["full_type_name"] = spark_full_type_name

        return spark_table_info

    def convert_from_spark_ddl(self, spark_table_info):
        # TODO: sep='\t' или sep= ' ' * 4

        mssql_table_info = spark_table_info

        mssql_table_name = mssql_table_info["table_name"]

        if mssql_table_info.get("schema", ""):
            mssql_table_name = f"{mssql_table_info['schema']}.{mssql_table_name}"

        if_not_exists = mssql_table_info.get("if_not_exists", "")

        if if_not_exists:
            if_not_exists = "IF NOT EXISTS "

        mssql_text_ddl = f"CREATE TABLE {if_not_exists}{mssql_table_name} (\n"

        for column in mssql_table_info["columns"]:

            spark_full_type_name = column["full_type_name"]
            mssql_full_type_name = self.to_mssql_converter.get(spark_full_type_name)

            if not mssql_full_type_name:
                raise Exception(
                    f"Spark datatype {spark_full_type_name} cannot be converted to MSSQL datatypes"
                )
            sep = " " * 4
            mssql_text_ddl += f"{sep}{column['name']} {mssql_full_type_name},\n"

        mssql_text_ddl += ");"

        return mssql_text_ddl
