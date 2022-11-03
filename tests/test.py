import pytest
from src.database.data_generator import DataGenerator
from src.pipelines.load import insert_data_to_db

import findspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType, TimestampType



############### DataGenerator Class tests ############################################################

def test_if_DataGenerator_sample_associado_method_returns_list():
    fake = DataGenerator()
    instance = fake.sample_associado(1)

    assert isinstance(instance, list)


def test_if_DataGenerator_sample_conta_method_returns_list():
    fake = DataGenerator()
    instance = fake.sample_conta(1)

    assert isinstance(instance, list)


def test_if_DataGenerator_sample_cartao_method_returns_list():
    fake = DataGenerator()
    instance = fake.sample_cartao(1)

    assert isinstance(instance, list)


def test_if_DataGenerator_sample_movimento_method_returns_list():
    fake = DataGenerator()
    instance = fake.sample_movimento(1)

    assert isinstance(instance, list)


############### PySpark Class tests ##################################################################

def test_if_returns_associado_dataframe(spark):
    fake = DataGenerator()

    associado_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='nome', dataType=StringType()),
            StructField(name='sobrenome', dataType=StringType()),
            StructField(name='idade', dataType=IntegerType()),
            StructField(name='email', dataType=StringType())
        ]
    )

    associado_data = fake.sample_associado(10)
    associado_dataframe = spark.createDataFrame(schema=associado_schema, data=associado_data)

    assert isinstance(associado_dataframe, DataFrame)


def test_if_returns_conta_dataframe(spark):
    fake = DataGenerator()

    conta_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='tipo', dataType=StringType()),
            StructField(name='data_criacao', dataType=TimestampType()),
            StructField(name='id_associado', dataType=IntegerType())
        ]
    )

    conta_data = fake.sample_conta(10)
    conta_dataframe = spark.createDataFrame(schema=conta_schema, data=conta_data)

    assert isinstance(conta_dataframe, DataFrame)


def test_if_returns_cartao_dataframe(spark):
    fake = DataGenerator()

    cartao_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='num_cartao', dataType=StringType()),
            StructField(name='nom_impresso', dataType=StringType()),
            StructField(name='data_criacao', dataType=TimestampType()),
            StructField(name='id_conta', dataType=IntegerType()),
            StructField(name='id_associado', dataType=IntegerType())
        ]
    )

    cartao_data = fake.sample_cartao(10)
    cartao_dataframe = spark.createDataFrame(schema=cartao_schema, data=cartao_data)

    assert isinstance(cartao_dataframe, DataFrame)


def test_if_returns_movimento_dataframe(spark):
    fake = DataGenerator()

    movimento_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='vlr_transacao', dataType=FloatType()),
            StructField(name='des_transacao', dataType=StringType()),
            StructField(name='data_movimento', dataType=TimestampType()),
            StructField(name='id_cartao', dataType=IntegerType())
        ]
    )

    movimento_data = fake.sample_movimento(10)
    movimento_dataframe = spark.createDataFrame(schema=movimento_schema, data=movimento_data)

    assert isinstance(movimento_dataframe, DataFrame)



def test_if_associado_dataframe_returns_valid_row_numbers(spark):
    fake = DataGenerator()

    associado_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='nome', dataType=StringType()),
            StructField(name='sobrenome', dataType=StringType()),
            StructField(name='idade', dataType=IntegerType()),
            StructField(name='email', dataType=StringType())
        ]
    )

    associado_data = fake.sample_associado(5)
    df_test = spark.createDataFrame(schema=associado_schema, data=associado_data)

    assert df_test.count() == 5


def test_if_conta_dataframe_returns_valid_row_numbers(spark):
    fake = DataGenerator()

    conta_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='tipo', dataType=StringType()),
            StructField(name='data_criacao', dataType=TimestampType()),
            StructField(name='id_associado', dataType=IntegerType())
        ]
    )

    conta_data = fake.sample_conta(5)
    df_test = spark.createDataFrame(schema=conta_schema, data=conta_data)

    assert df_test.count() == 5


def test_if_cartao_dataframe_returns_valid_row_numbers(spark):
    fake = DataGenerator()

    cartao_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='num_cartao', dataType=StringType()),
            StructField(name='nom_impresso', dataType=StringType()),
            StructField(name='data_criacao', dataType=TimestampType()),
            StructField(name='id_conta', dataType=IntegerType()),
            StructField(name='id_associado', dataType=IntegerType())
        ]
    )

    cartao_data = fake.sample_cartao(5)
    df_test = spark.createDataFrame(schema=cartao_schema, data=cartao_data)

    assert df_test.count() == 5


def test_if_movimento_dataframe_returns_valid_row_numbers(spark):
    fake = DataGenerator()

    movimento_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='vlr_transacao', dataType=FloatType()),
            StructField(name='des_transacao', dataType=StringType()),
            StructField(name='data_movimento', dataType=TimestampType()),
            StructField(name='id_cartao', dataType=IntegerType())
        ]
    )

    movimento_data = fake.sample_movimento(5)
    df_test = spark.createDataFrame(schema=movimento_schema, data=movimento_data)

    assert df_test.count() == 5

    