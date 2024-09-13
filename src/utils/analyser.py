from pyspark.sql import SparkSession, DataFrame
from src.log_manager import logger
from typing import List, Dict

spark = SparkSession.builder.appName("data_filtering").getOrCreate()


def filter_by_country(df: DataFrame, countries: List[str]) -> DataFrame:
    """
    Filter a DataFrame to keep only clients from the specified countries.

    :param df: :class:`pyspark.sql.DataFrame`
        The input DataFrame containing client data.
    :param countries: list of str
        A list of countries to filter the clients by.

    :return: :class:`pyspark.sql.DataFrame`
        A filtered DataFrame containing clients only from the specified countries.

    :raises ValueError: If the DataFrame does not contain a 'country' column.

    :example:

    >>> df_filtered = filter_by_country(df, ["United Kingdom", "Netherlands"])

    """
    logger.info(f"Filtering DataFrame by countries: {countries}")
    filtered_df: DataFrame  = df.filter(df.country.isin(countries))

    logger.info(f"Filtered DataFrame. Original count: {df.count()}, Filtered count: {filtered_df.count()}")

    return filtered_df


def rename_columns(df1: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
    """
    Rename columns in the DataFrame for easier readability for business users.

    :param df: :class:`pyspark.sql.DataFrame`
        The input DataFrame whose columns need to be renamed.
    :param rename_map: dict
        A dictionary mapping old column names (keys) to new column names (values).

    :return: :class:`pyspark.sql.DataFrame`
        The DataFrame with renamed columns.

    :example:

    >>> rename_map = {"id": "client_identifier", "btc_a": "bitcoin_address"}
    >>> df_renamed = rename_columns(df_account_info, rename_map)

    """
    logger.info(f"Renaming columns: {rename_map}")

    original_columns: List[str] = df1.columns

    for old_name, new_name in rename_map.items():
        if old_name in df1.columns:
            df1 = df1.withColumnRenamed(old_name, new_name)
        else:
            logger.warning(f"Column '{old_name}' not found in DataFrame. Skipping renaming.")

    logger.info(f"Columns renamed.")

    return df1