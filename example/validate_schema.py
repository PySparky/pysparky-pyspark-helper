from pyspark.sql import DataFrame, SparkSession
from pysparky.decorator import validate_schema

def main():
    # Initialize a small local PySpark session
    spark = SparkSession.builder.appName("Validate Schema Example").getOrCreate()

    # Create sample data
    data = [(1, 100.0), (2, 250.0), (3, 50.0)]
    columns = ["user_id", "raw_revenue"]
    df = spark.createDataFrame(data, columns)

    print("Initial DataFrame:")
    df.show()

    # Define the decorated function
    @validate_schema(inputs=["user_id", "raw_revenue"], outputs=["net_revenue"])
    def calculate_net_revenue(df: DataFrame) -> DataFrame:
        return df.withColumn("net_revenue", df["raw_revenue"] * 0.8)

    # Apply the function
    print("Applying 'calculate_net_revenue'...")
    result_df = calculate_net_revenue(df)

    print("Resulting DataFrame:")
    result_df.show()

    print("Example completed successfully!")

if __name__ == "__main__":
    main()
