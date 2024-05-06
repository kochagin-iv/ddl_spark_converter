from ddl_spark_converter.db_converter.IConverter import IConverter
from ddl_spark_converter.db_converter.utils import generate_full_type_name
from ddl_spark_converter.db_from_spark_datatype_conversion_info.oracle import (
    datatypes as spark_oracle_datatypes,
)
from ddl_spark_converter.db_to_spark_datatype_conversion_info.oracle import (
    datatypes as oracle_spark_datatypes,
)


class OracleConverter(IConverter):
    def __init__(self):
        self.to_spark_converter = oracle_spark_datatypes
        self.to_oracle_converter = spark_oracle_datatypes

    def convert_to_spark_ddl(self, ddl_text):
        spark_table_info = ddl_text

        for i, column in enumerate(spark_table_info["columns"]):

            oracle_full_type_name = generate_full_type_name(column=column)
            spark_full_type_name = self.to_spark_converter.get(oracle_full_type_name)

            if not spark_full_type_name:
                raise Exception(
                    f"Oracle datatype {oracle_full_type_name} cannot be converted to Spark datatypes"
                )

            spark_table_info["columns"][i]["full_type_name"] = spark_full_type_name
        return spark_table_info

    def convert_from_spark_ddl(self, spark_table_info):

        oracle_table_info = spark_table_info
        oracle_table_name = oracle_table_info["table_name"]

        if oracle_table_info.get("schema", ""):
            oracle_table_name = f"{oracle_table_info['schema']}.{oracle_table_name}"

        if_not_exists = oracle_table_info.get("if_not_exists", "")

        if if_not_exists:
            if_not_exists = "IF NOT EXISTS "

        oracle_text_ddl = f"CREATE TABLE {if_not_exists}{oracle_table_name} (\n"

        for column in oracle_table_info["columns"]:

            spark_full_type_name = column["full_type_name"]
            oracle_full_type_name = self.to_oracle_converter.get(spark_full_type_name)

            if not oracle_full_type_name:
                raise Exception(
                    f"Spark datatype {spark_full_type_name} cannot be converted to Oracle datatypes"
                )
            sep = " " * 4
            oracle_text_ddl += f"{sep}{column['name']} {oracle_full_type_name},\n"

        oracle_text_ddl += ");"

        return oracle_text_ddl
