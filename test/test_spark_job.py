import pytest
from pyspark.sql import SparkSession

from app.spark_job_re import BatchProcess

def test_number_of_columns_before_write_process():
    btch = BatchProcess()
    spark = btch.spark_process()
    df = btch.readData(spark=spark)
    flattened = btch.explode_to_flattened(df)
    query = btch.create_query_df(flattened=flattened)
    
    # Assert the number of columns before the write process
    assert len(query.columns) == 9




