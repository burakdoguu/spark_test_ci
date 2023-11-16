import pytest
from pyspark.sql import SparkSession
import sys

from spark_job_re import BatchProcess

@pytest.fixture(scope='session')
def spark_session():
    spark = SparkSession \
            .builder \
            .appName("Spark Unit Test") \
            .master("spark://spark:7077") \
            .getOrCreate() 
    return spark    

def test_column_count(spark_session):
    check_df = BatchProcess().create_query_df()
    expected_df = len(check_df.columns)

    assert expected_df == 9

