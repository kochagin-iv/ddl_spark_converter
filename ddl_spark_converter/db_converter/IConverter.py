from abc import ABC, abstractmethod
from typing import Any, Dict


class IConverter(ABC):
    """
    Абстрактный класс конвертера, используется в конкретных реплизациях конвертеров
    """

    # TODO: подумать
    # Возможно, стоит разбить на 5 функций, тогда будет 2 на конвертацию типа столбца(to_ddl), 2 на конвертацию имени столбца и 1 - объединяющая
    @abstractmethod
    def convert_to_spark_ddl(
        self, dbms_ddl_dict: Dict[str, Any], **kwargs
    ) -> Dict[str, Any]:
        """
        Функция переводит типы данных в словаре dbms_ddl_dict с типов субд dbms в типы Spark
        """
        pass

    @abstractmethod
    def convert_from_spark_ddl(self, spark_ddl_dict: Dict[str, Any], **kwargs) -> str:
        """
        Функция переводит ddl в словаре spark_ddl_dict в строковое ddl на стороне субд dbms
        """
        pass
