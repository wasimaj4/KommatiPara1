from pyspark.sql import SparkSession
from utils.analyser import filter_by_country,rename_columns
from utils.log_manager import logger
spark = SparkSession.builder.appName("data_filtering").getOrCreate()


def analysis_fun(link1, link2, country):
    """
    Analyze client and account data by filtering, renaming columns, and performing a join operation.

    The function reads two CSV files, filters the clients by specified countries,
    renames some columns in the account information, and performs an inner join between
    the two DataFrames. The final cleaned DataFrame is saved to the `client_data` directory.

    :param link1: str
        Path to the CSV file containing client data.
    :param link2: str
        Path to the CSV file containing account information.
    :param country: list of str
        A list of countries to filter the clients by.

    :return: :class:`pyspark.sql.DataFrame`
        A cleaned and joined DataFrame with relevant client and account information.

    :raises FileNotFoundError: If one of the CSV file paths is invalid.

    :example:

    >>> df = analysis_fun("clients.csv", "accounts.csv", ["United Kingdom", "Netherlands"])
    >>> df.show()

    """
    logger.info("Read CSV files")
    df_clients = spark.read.csv(link1, header=True, inferSchema=True)
    df_account_info = spark.read.csv(link2, header=True, inferSchema=True)
    logger.info("Filtering and renaming")
    df_clients = filter_by_country(df_clients, country)




    logger.info("Drop unnecessary columns from both DataFrames")
    df_clients = df_clients.drop("first_name", "last_name", "country")
    df_account_info = df_account_info.drop("credit_card")
