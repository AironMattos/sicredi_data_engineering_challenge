from pyspark.sql import SparkSession
import findspark


def extract_to_new_table(path):
    '''
        Extract data from database tables, create tempViews and merge in 'movimento_flat' dataframe
    '''

    # Init SparkSession
    findspark.init()

    spark = SparkSession \
        .builder \
        .appName("extract") \
        .config("spark.jars", "src/drivers/postgresql-42.5.0.jar") \
        .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    # Read base tables from database
    reader = spark.read\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/sicredi_data_challenge") \
        .option("driver", "org.postgresql.Driver")\
        .option("user", "root")\
        .option("password", "root")

    tablename_list = ["associado", "conta", "cartao", "movimento"]

    # Create TempView using base tables
    for tablename in tablename_list:
        reader.option("dbtable", tablename).load().createTempView(tablename)

    # Create movimento_flat dataframe

    movimento_flat = spark.sql("""
                    SELECT 
                        t1.nome as nome_associado,
                        t1.sobrenome as sobrenome_associado,
                        t1.idade as idade_associado,
                        t2.vlr_transacao as vlr_transacao_movimento,
                        t2.des_transacao as des_transacao_movimento,
                        t2.data_movimento,
                        t3.num_cartao as numero_cartao,
                        t3.nom_impresso as nome_impresso_cartao,
                        t3.data_criacao as data_criacao_cartao,
                        t4.tipo as tipo_conta,
                        t4.data_criacao as data_criacao_conta
                    FROM 
                        associado as t1
                    JOIN
                        movimento as t2,
                        cartao as t3,
                        conta as t4
                    WHERE
                        t1.id = t4.id_associado and
                        t4.id = t3.id_associado and
                        t3.id = t2.id_cartao
                    """)

    # Write movimento_flat dataframe in a csv file
    movimento_flat.coalesce(1).write.csv(path=path, sep=';', header=True, mode='overwrite')