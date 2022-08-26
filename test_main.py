import main

from pyspark.sql import SparkSession


def equal_dataframes(df1, df2):
    # Make sure dataframes have the same columns, sort first.
    df1_cols = df1.columns
    df2_cols = df2.columns
    df1_cols.sort()
    df2_cols.sort()

    # Check if columns are equal
    if not df1_cols == df2_cols:
        logger.info(
            "Columns are not equal: df1 has columns {}, "
            "while df2 has columns {}".format(df1_cols, df2_cols)
        )
        return False

    # Compare dfs
    df1 = df1.select(df1_cols)
    df2 = df2.select(df2_cols)
    return (
        df1.count() - df2.count()
        == df1.subtract(df2).count()
        == df2.subtract(df1).count()
        == 0
    )


def test_clean_dataframe():
    spark = SparkSession.builder.getOrCreate()

    test_df = spark.createDataFrame(data=[["x", "y"]], schema=["col_a", "col_b"])

    test_df = main.clean_dataframe(test_df, ["col_a"])

    validation_df = spark.createDataFrame(data=[["x"]], schema=["col_a"])

    assert equal_dataframes(test_df, validation_df)
