from typing import Any, Dict, List

from ddl_spark_converter.db_converter.IConverter import IConverter
from ddl_spark_converter.db_converter.utils import generate_full_type_name
from ddl_spark_converter.db_from_spark_datatype_conversion_info.mysql import (
    datatypes as spark_mysql_datatypes,
)
from ddl_spark_converter.db_to_spark_datatype_conversion_info.mysql import (
    datatypes as mysql_spark_datatypes,
)


class MYSQLConverter(IConverter):
    def __init__(self):
        self.to_spark_converter = mysql_spark_datatypes
        self.to_mysql_converter = spark_mysql_datatypes

    def convert_to_spark_ddl(self, ddl_text: Dict[str, Any]) -> Dict[str, Any]:
        """
        Функция изменяет типы данных MYSQL на соответствующие типы данных Spark

        :param: ddl_text - ddl таблицы после обработки парсером(передается в виде словаря)

        :return: Тот же словарь ddl_text с доп. полем full_type_name - имя типа данных Spark

        """
        spark_table_info = ddl_text

        for i, column in enumerate(spark_table_info["columns"]):

            mysql_full_type_name = generate_full_type_name(column=column)
            spark_full_type_name = self.to_spark_converter.get(mysql_full_type_name)

            if not spark_full_type_name:
                raise Exception(
                    f"MYSQL datatype {mysql_full_type_name} cannot be converted to Spark datatypes"
                )

            spark_table_info["columns"][i]["full_type_name"] = spark_full_type_name
        return spark_table_info

    def convert_from_spark_ddl(self, spark_table_info):

        mysql_table_info = spark_table_info

        mysql_table_name = mysql_table_info["table_name"]

        if mysql_table_info.get("schema", ""):
            mysql_table_name = f"{mysql_table_info['schema']}.{mysql_table_name}"

        if_not_exists = mysql_table_info.get("if_not_exists", "")

        if if_not_exists:
            if_not_exists = "IF NOT EXISTS "

        mysql_text_ddl = f"CREATE TABLE {if_not_exists}{mysql_table_name} (\n"

        for column in mysql_table_info["columns"]:

            spark_full_type_name = column["full_type_name"]
            mysql_full_type_name = self.to_mysql_converter.get(spark_full_type_name)

            if not mysql_full_type_name:
                raise Exception(
                    f"Spark datatype {spark_full_type_name} cannot be converted to MYSQL datatypes"
                )

            mysql_text_ddl += f"\t{column['name']} {mysql_full_type_name}, \n"

        mysql_text_ddl += ");"

        return mysql_text_ddl
