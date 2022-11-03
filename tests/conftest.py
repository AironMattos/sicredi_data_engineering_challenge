import pytest
from pyspark.sql import SparkSession
import findspark

@pytest.fixture(scope='session')
def spark():
    findspark.init()
    
    return SparkSession.builder \
      .master("local") \
      .appName("test_pipelines") \
      .getOrCreate()