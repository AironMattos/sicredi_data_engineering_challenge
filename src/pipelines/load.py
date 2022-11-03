from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType, TimestampType
import findspark
from src.database.data_generator import DataGenerator


def insert_data_to_db(rows_number):
    data_generator = DataGenerator()

    # Init SparkSession
    findspark.init()

    spark = SparkSession \
        .builder \
        .appName("base_tables") \
        .config("spark.jars", "src/drivers/postgresql-42.5.0.jar") \
        .getOrCreate()

    # Create tables schema

    associado_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='nome', dataType=StringType()),
            StructField(name='sobrenome', dataType=StringType()),
            StructField(name='idade', dataType=IntegerType()),
            StructField(name='email', dataType=StringType())
        ]
    )

    conta_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='tipo', dataType=StringType()),
            StructField(name='data_criacao', dataType=TimestampType()),
            StructField(name='id_associado', dataType=IntegerType())
        ]
    )

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

    movimento_schema = StructType(
        [
            StructField(name='id', dataType=IntegerType()),
            StructField(name='vlr_transacao', dataType=FloatType()),
            StructField(name='des_transacao', dataType=StringType()),
            StructField(name='data_movimento', dataType=TimestampType()),
            StructField(name='id_cartao', dataType=IntegerType())
        ]
    )

    # Create sample data
    associado_data = data_generator.sample_associado(rows_number)
    conta_data = data_generator.sample_conta(rows_number)
    cartao_data = data_generator.sample_cartao(rows_number)
    movimento_data = data_generator.sample_movimento(rows_number)


    # Create dataframes to insert in database
    associado = spark.createDataFrame(schema=associado_schema, data=associado_data)
    conta = spark.createDataFrame(schema=conta_schema, data=conta_data)
    cartao = spark.createDataFrame(schema=cartao_schema, data=cartao_data)
    movimento = spark.createDataFrame(schema=movimento_schema, data=movimento_data)


    # Dataframes to table
    tables_to_load = {"associado": associado, "conta": conta, "cartao": cartao, "movimento": movimento}

    for table in tables_to_load.items():
        table[1].write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/sicredi_data_challenge") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", str(table[0])) \
            .option("user", "root") \
            .option("password", "root") \
            .mode("append") \
            .save()