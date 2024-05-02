from ddlsparkconverter.db_from_spark_datatype_conversion_info.oracle import (
    datatypes as spark_oracle_datatypes,
)
from ddlsparkconverter.db_to_spark_datatype_conversion_info.oracle import (
    datatypes as oracle_spark_datatypes,
)

from ddlsparkconverter.db_converter.IConverter import IConverter
from ddlsparkconverter.db_converter.utils import generate_full_type_name


class OracleConverter(IConverter):
    def __init__(self):
        self.to_spark_converter = oracle_spark_datatypes
        self.to_oracle_converter = spark_oracle_datatypes

    def convert_to_spark_ddl(self, ddl_text):
        spark_table_info = ddl_text[0]

        for i, column in enumerate(spark_table_info["columns"]):

            column_full_type_name = generate_full_type_name(column=column)

            spark_table_info["columns"][i]["full_type_name"] = self.to_spark_converter[
                column_full_type_name
            ]
        return spark_table_info

    def convert_from_spark_ddl(self, spark_table_info):

        oracle_table_info = spark_table_info

        oracle_table_name = oracle_table_info["table_name"]

        if oracle_table_info.get("schema", ""):
            oracle_table_name = f"{oracle_table_info['schema']}.{oracle_table_name}"

        oracle_text_ddl = f"CREATE TABLE {oracle_table_name} (\n"

        for column in oracle_table_info["columns"]:

            column_full_name = column["full_type_name"]

            column_full_type_name = self.to_oracle_converter[column_full_name]

            oracle_text_ddl += f"\t{column['name']} {column_full_type_name}, \n"

        oracle_text_ddl += ");"

        return oracle_text_ddl
