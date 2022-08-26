from pyspark.sql import SparkSession


def get_raw_dataframe():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(
        data=[
            ["Hello, World!", 42],
        ],
        schema=["string_col", "int_col"],
    )

    return df


def clean_dataframe(df, relevant_cols):
    return df.select(relevant_cols)


def run_pipeline():
    df = get_raw_dataframe()

    print("Raw dataframe:")
    df.show()

    df = clean_dataframe(df, ["string_col"])

    print("Clean dataframe:")
    df.show()


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
