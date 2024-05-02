from simple_ddl_parser import DDLParser

from ddlsparkconverter.db_converter.mysql import MYSQLConverter
from ddlsparkconverter.db_converter.mssql import MSSQLConverter
from ddlsparkconverter.db_converter.oracle import OracleConverter


class DatabaseConverter:
    def __init__(self, source_db, target_db, ddl_text):
        self.source_db = source_db
        self.target_db = target_db
        self.ddl_text = ddl_text

    def _convert_to_spark_ddl(self, ddl_text):
        converter_to_spark = ConverterDispatcher.dispatch(db_name=self.source_db)()
        parser = ParserDispatcher.dispatch(db_name=self.source_db)
        output_mode = OutputModeDispatcher.dispatch(db_name=self.source_db)

        parsed_ddl = parser(ddl_text).run(output_mode=output_mode)

        return converter_to_spark.convert_to_spark_ddl(parsed_ddl)

    def _convert_from_spark_ddl(self, spark_table_info):
        converter_from_spark = ConverterDispatcher.dispatch(db_name=self.target_db)()

        return converter_from_spark.convert_from_spark_ddl(spark_table_info)

    def run(self) -> str:
        ddl_text = self.ddl_text
        spark_table_info = self._convert_to_spark_ddl(ddl_text=ddl_text)

        target_table_ddl = self._convert_from_spark_ddl(
            spark_table_info=spark_table_info
        )

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
        }

        return parsers[db_name]


class ConverterDispatcher:

    @staticmethod
    def dispatch(db_name: str):
        converters = {
            "oracle": OracleConverter,
            "mysql": MYSQLConverter,
            "mssql": MSSQLConverter,
        }

        return converters[db_name]


class OutputModeDispatcher:

    @staticmethod
    def dispatch(db_name: str):
        output_mode = {
            "oracle": "oracle",
            "mssql": "mssql",
            "mysql": "mysql",
            "postgres": "postgres",
            "greenplum": "postgres",
            "hive": "hql",
        }

        return output_mode[db_name]
