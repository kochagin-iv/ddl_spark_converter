from ddlsparkconverter.db_from_spark_datatype_conversion_info.mysql import (
    datatypes as spark_mysql_datatypes,
)
from ddlsparkconverter.db_to_spark_datatype_conversion_info.mysql import (
    datatypes as mysql_spark_datatypes,
)

from ddlsparkconverter.db_converter.IConverter import IConverter
from ddlsparkconverter.db_converter.utils import generate_full_type_name


class MYSQLConverter(IConverter):
    def __init__(self):
        self.to_spark_converter = mysql_spark_datatypes
        self.to_mysql_converter = spark_mysql_datatypes

    def convert_to_spark_ddl(self, ddl_text):
        spark_table_info = ddl_text[0]

        for i, column in enumerate(spark_table_info["columns"]):

            column_full_type_name = generate_full_type_name(column=column)

            spark_table_info["columns"][i]["full_type_name"] = self.to_spark_converter[
                column_full_type_name
            ]
        return spark_table_info

    def convert_from_spark_ddl(self, spark_table_info):

        mysql_table_info = spark_table_info

        mysql_table_name = mysql_table_info["table_name"]

        if mysql_table_info.get("schema", ""):
            mysql_table_name = f"{mysql_table_info['schema']}.{mysql_table_name}"

        mysql_text_ddl = f"CREATE TABLE {mysql_table_name} (\n"

        for column in mysql_table_info["columns"]:

            column_full_name = column["full_type_name"]

            column_full_type_name = self.to_mysql_converter[column_full_name]

            mysql_text_ddl += f"\t{column['name']} {column_full_type_name}, \n"

        mysql_text_ddl += ");"

        return mysql_text_ddl
