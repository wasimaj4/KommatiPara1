
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType

from src.log_manager import logger
from src.utils.analyser import filter_by_country,rename_columns
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = (SparkSession.builder.master("local").appName("tester").getOrCreate())

countries_test = ["Netherlands", "United Kingdom"]
sample_data1 = [
    (1, "John", "Doe", "john.doe@example.com", "Netherlands"),
    (2, "Jane", "Smith", "jane.smith@example.com", "Canada"),
    (3, "Michael", "Johnson", "michael.johnson@example.com", "United Kingdom"),
    (4, "Emily", "Brown", "emily.brown@example.com", "Australia"),
    (5, "David", "Lee", "david.lee@example.com", "United Kingdom")
]

sample_data1_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True)
])


df_one = spark.createDataFrame(sample_data1, schema=sample_data1_schema)

sample_data2 = [
    (1, "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2", "VISA", 4532015112830366),
    (2, "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", "MASTERCARD", 5425233430109903),
    (3, "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq", "AMEX", 374245455400126),
    (4, "1BoatSLRHtKNngkdXEeobR76b53LETtpyT", "DISCOVER", 6011000990139424),
    (5, "3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd", "VISA", 4916338506082832)
]

sample_data2_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("btc_a", StringType(), True),
    StructField("cc_t", StringType(), True),
    StructField("cc_n", LongType(), True)
])

df_two = spark.createDataFrame(sample_data2, schema=sample_data2_schema)
def test_filtering() -> None:
    """
    Test the filter_by_country function.

    This function tests the filter_by_country function by applying it to a sample DataFrame
    and comparing the result with an expected output.

    :return: None
    :raises: AssertionError if the actual output doesn't match the expected output
    """
    logger.info("Starting test_filtering function")
    actual_df1= filter_by_country(df_one,countries_test)

    expected_data1 = [
        (1, "John", "Doe", "john.doe@example.com", "Netherlands"),
        (3, "Michael", "Johnson", "michael.johnson@example.com", "United Kingdom"),
        (5, "David", "Lee", "david.lee@example.com", "United Kingdom")
    ]
    expected_data_schema1 = StructType([
        StructField("id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True)
    ])
    expected_df1 = spark.createDataFrame(expected_data1, schema=expected_data_schema1)

    logger.info("Asserting DataFrame equality")
    assert_df_equality(actual_df1, expected_df1)

    logger.info("Asserting schema equality")
    assert_schema_equality(actual_df1.schema, expected_data_schema1)

    logger.info("test_filtering function completed successfully")


def test_renaming() -> None:
    """
        Test the rename_columns function.

        This function tests the rename_columns function by applying it to a sample DataFrame
        and comparing the result with an expected output.

        :return: None
        :raises: AssertionError if the actual output doesn't match the expected output
        """
    logger.info("Starting test_renaming function")
    actual_df2= rename_columns(df_two, {"id": "client_identifier", "btc_a": "bitcoin_address","cc_t": "account_type", "cc_n": "account_number"})
    expected_data2 = [
        (1, "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2", "VISA", 4532015112830366),
        (2, "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy", "MASTERCARD", 5425233430109903),
        (3, "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq", "AMEX", 374245455400126),
        (4, "1BoatSLRHtKNngkdXEeobR76b53LETtpyT", "DISCOVER", 6011000990139424),
        (5, "3E8ociqZa9mZUSwGdSmAEMAoAxBK3FNDcd", "VISA", 4916338506082832)
    ]


    expected_data2_schema = StructType([
        StructField("client_identifier", IntegerType(), False),
        StructField("bitcoin_address", StringType(), True),
        StructField("account_type", StringType(), True),
        StructField("account_number", LongType(), True)
    ])

    expected_df2 = spark.createDataFrame(expected_data2, schema=expected_data2_schema)
    logger.info("Asserting DataFrame equality")
    assert_df_equality(actual_df2, expected_df2)
    logger.info("Asserting schema equality")
    assert_schema_equality(actual_df2.schema, expected_data2_schema)
    logger.info("test_renaming function completed successfully")
