# DDL_SPARK_CONVERTER

## Проект для конвертации DDL таблиц между разными СУБД при помощи Spark

На данный момент поддерживается конвертация между:
* MSSQL
* ORACLE
* MYSQL

Планируется поддержка:

* POSTGRESQL
* GREENPLUM
* MONGODB
* CLICKHOUSE
* HIVE

Пока поддерживается только конвертация типов между СУБД и не поддерживаются переносы аттрибутов DDL типа партиционирования, location, компрессии.

## Пример работы
```python
from ddl_spark_converter.main_converter import DatabaseConverter

ddl = """
CREATE TABLE IF NOT EXISTS test.Persons (
    PersonID FLOAT,
    LastName varchar,
    Money DECIMAL(5),
    time TIME,
    xml XML,
);
"""

converter = DatabaseConverter(source_db="mssql", target_db="oracle", ddl_text=ddl)

result_ddl = converter.run()

expected_ddl = """CREATE TABLE IF NOT EXISTS test.Persons (
    PersonID NUMBER(19, 4),
    LastName CLOB,
    Money NUMBER(5, 0),
    time TIMESTAMP(6),
    xml CLOB,
);"""

assert result_ddl == expected_ddl

```

## Структура проекта:

```
    .
    ├── ddl_spark_converter
        ├── db_converter
        |   └── {name_dbms}.py - код конвертера для name_dbms
        ├── db_from_spark_datatype_conversion_info
        |   ├── {name_dbms}.py - словарь с информацией о конвертации типа данных name_dbms в тип данных Spark
        ├── db_to_spark_datatype_conversion_info
        |   ├── {name_dbms}.py - словарь с информацией о конвертации типа данных Spark в тип данных name_dbms
        └── main_converter.py - содержит реализацию DatabaseConverter и dispatchers для выбора типа парсера DDL
    └── tests
        ├── input_ddl - содержит txt файлы с DDL для тестов для разных СУБД (подается на вход)
        ├── output_ddl - содержит txt файлы с ожидаемыми DDL для тестов для разных СУБД
        └── test_{name_dbms_from}_{name_dbms_to}.py - содержит код тестов для конвертации DDL из name_dbms_from в name_dbms_to


```