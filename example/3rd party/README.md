# How to use PySpark with 3rd Party App

## MongoDB
```python
from pysparky.spark_configs import mongodb_spark_config

(
    SparkSession.builder.appName("myApp")
        .config(
            map=mongodb_spark_config
        )
        .getOrCreate()
)

(
    spark.read.format("mongodb")
        .option("database", database)
        .option("collection", collection)
        .load()
)
```

## AWS S3
```python
from pysparky.spark_configs import aws_s3_spark_config

(
    SparkSession.builder.appName("myApp")
        .config(
            map=aws_s3_spark_config
        )
        .getOrCreate()
)

(
    spark.read.csv("s3a://<bucket_name>/<path>", header=True, inferSchema=True, sep=";")
)
```


## AWS Athena
```python
from pysparky.spark_configs import aws_athena_spark_config

(
    SparkSession.builder.appName("myApp")
        .config(
            map=aws_athena_spark_config
        )
        .getOrCreate()
)

spark.read.table("iceberg_catalog.<database>.<table>").select("id").limit(1).show()
spark.sql(f"SELECT id FROM iceberg_catalog.<database>.<table> LIMIT 1").show()
spark.sql(f"SELECT id FROM `iceberg_catalog`.`<database>`.`<table>` LIMIT 1").show()
```
