from typing import Any, Dict

from simple_ddl_parser import DDLParser

from ddl_spark_converter.db_converter.IConverter import IConverter
from ddl_spark_converter.db_converter.mssql import MSSQLConverter
from ddl_spark_converter.db_converter.mysql import MYSQLConverter
from ddl_spark_converter.db_converter.oracle import OracleConverter


class DatabaseConverter:
    """
    Основной класс конвертера, объединяющий работу конвертеров из разных СУБД
    """

    def __init__(self, source_db: str, target_db: str, ddl_text: str):
        # TODO: вынести source_db, target_db, другие параметры в класс конфигов + добавить валидацию
        """
        :param: source_db - имя СУБД источника
        :param: target_db - имя СУБД приемника
        :param: ddl_text - строка-DDL на стороне источника
        """

        self.source_db = source_db
        self.target_db = target_db
        self.ddl_text = ddl_text

    def _convert_to_spark_ddl(self, ddl_text: str) -> Dict[str, Any]:
        """
        :param: ddl_text - строка-DDL на стороне источника

        :return: Словарь с распаршенным DDL и типами данных Spark
        """
        converter = ConverterDispatcher.dispatch(db_name=self.source_db)()
        parser = ParserDispatcher.dispatch(db_name=self.source_db)
        output_mode = OutputModeDispatcher.dispatch(db_name=self.source_db)

        parsed_ddl = parser(ddl_text).run(output_mode=output_mode)[0]

        return converter.convert_to_spark_ddl(parsed_ddl)

    def _convert_from_spark_ddl(self, spark_ddl_dict: Dict[str, Any]) -> str:
        """
        :param: spark_ddl_dict Словарь с распаршенным DDL и типами данных Spark

        :return: строка-DDL с корретными типами данных на стороне СУБД-приемника
        """

        converter = ConverterDispatcher.dispatch(db_name=self.target_db)()

        return converter.convert_from_spark_ddl(spark_ddl_dict)

    def run(self) -> str:
        """
        :return: строка-DDL с корретными типами данных на стороне СУБД-приемника
        """

        ddl_text = self.ddl_text
        spark_ddl_dict = self._convert_to_spark_ddl(ddl_text=ddl_text)

        target_table_ddl = self._convert_from_spark_ddl(spark_ddl_dict=spark_ddl_dict)

        return target_table_ddl


class ParserDispatcher:
    @staticmethod
    def dispatch(db_name: str):
        parsers = {
            "oracle": DDLParser,
            "mssql": DDLParser,
            "mysql": DDLParser,
            "postgres": DDLParser,
            "greenplum": DDLParser,
            # TODO: "clickhouse" - будет иной парсер
        }

        return parsers[db_name]


class ConverterDispatcher:

    @staticmethod
    def dispatch(db_name: str) -> IConverter:
        converters = {
            "oracle": OracleConverter,
            "mysql": MYSQLConverter,
            "mssql": MSSQLConverter,
        }

        return converters[db_name]


class OutputModeDispatcher:

    @staticmethod
    def dispatch(db_name: str) -> str:
        output_mode = {
            "oracle": "oracle",
            "mssql": "mssql",
            "mysql": "mysql",
            "postgres": "postgres",
            "greenplum": "postgres",
            "hive": "hql",
        }

        return output_mode[db_name]
