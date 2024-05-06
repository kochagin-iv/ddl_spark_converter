from typing import Any, Dict

from ddl_spark_converter.db_converter.IConverter import IConverter
from ddl_spark_converter.db_converter.utils import generate_full_type_name
from ddl_spark_converter.db_from_spark_datatype_conversion_info.mssql import (
    datatypes as spark_mssql_datatypes,
)
from ddl_spark_converter.db_to_spark_datatype_conversion_info.mssql import (
    datatypes as mssql_spark_datatypes,
)


class MSSQLConverter(IConverter):
    """
    Класс конвертера для MSSQL

    Имеет 2 функции
        1) convert_to_spark_ddl - переводит типы данных MSSQL в типы данных Spark
        2) convert_to_spark_ddl - переводит типы данных Spark в типы данных MSSQL
    """

    def __init__(self):
        self.to_spark_converter = mssql_spark_datatypes
        self.to_mssql_converter = spark_mssql_datatypes

    def convert_to_spark_ddl(self, mssql_ddl_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Функция изменяет типы данных MSSQL на соответствующие типы данных Spark

        :param: mssql_ddl_dict - ddl таблицы после обработки парсером(передается в виде словаря)

        :return: Тот же словарь mssql_ddl_dict с доп. полем full_type_name - имя типа данных Spark

        """
        spark_ddl_dict = mssql_ddl_dict

        for i, column in enumerate(spark_ddl_dict["columns"]):

            mssql_full_type_name = generate_full_type_name(column=column)
            spark_full_type_name = self.to_spark_converter.get(mssql_full_type_name)

            if not spark_full_type_name:
                raise Exception(
                    f"MSSQL datatype {mssql_full_type_name} cannot be converted to Spark datatypes"
                )

            spark_ddl_dict["columns"][i]["full_type_name"] = spark_full_type_name

        return spark_ddl_dict

    def convert_from_spark_ddl(self, spark_ddl_dict: Dict[str, Any]) -> str:
        # TODO: sep='\t' или sep= ' ' * 4, наверное, стоит сделать параметром
        """
        Функция изменяет типы данных Spark на соответствующие типы данных MSSQL

        :param: spark_ddl_dict - ddl таблицы после обработки функцией convert_to_spark_ddl(передается в виде словаря)

        :return: DDL MSSQL таблицы в виде строки

        """

        mssql_table_info = spark_ddl_dict

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
