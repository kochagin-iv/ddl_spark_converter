from abc import ABC, abstractmethod


class IConverter(ABC):
    # TODO: подумать
    # Возможно, стоит разбить на 5 функций, тогда будет 2 на конвертацию типа столбца, 2 на конвертацию имени столбца и 1 - объединяющая
    @abstractmethod
    def convert_to_spark_ddl(self, ddl_text, **kwargs):
        pass

    @abstractmethod
    def convert_from_spark_ddl(self, spark_table_info, **kwargs):
        pass
