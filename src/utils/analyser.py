from pyspark.sql import SparkSession, DataFrame
from log_manager import logger
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

    if 'country' not in df.columns:
        logger.error("DataFrame does not contain a 'country' column")
        raise ValueError("DataFrame does not contain a 'country' column")

    filtered_df: DataFrame  = df.filter(df.country.isin(countries))

    logger.info(f"Filtered DataFrame. Original count: {df.count()}, Filtered count: {filtered_df.count()}")

    return filtered_df
