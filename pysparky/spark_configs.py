iceberg_spark_config = {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.spark_catalog.type": "hive",
    "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.local.warehouse": "./warehouse",
    "spark.sql.catalog.local.type": "hadoop",
}

aws_athena_spark_config = {
    "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    "spark.executor.heartbeatInterval": "600s",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.11,org.apache.iceberg:iceberg-aws-bundle:1.7.12,software.amazon.awssdk:url-connection-client:2.27.213,org.apache.hadoop:hadoop-aws:3.4.14",
    "spark.network.timeout": "700s",
    "spark.sql.broadcastTimeout": "3600s",
    "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.iceberg_catalog.glue.id": "123456789012",
    "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg_catalog.warehouse": "s3://<bucket_name>",
    "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.spark_catalog.type": "hive",
    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.defaultCatalog": "iceberg_catalog",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.session.timeZone": "UTC",
}

aws_s3_spark_config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
}

mongodb_spark_config = {
    "spark.mongodb.read.connection.uri": "<connection_string>",
    "spark.mongodb.write.connection.uri": "<connection_string>",
    "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
}


catalog_name = "s3tablescatalog/tablebucket"
aws_s3_tables_spark_config = {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,software.amazon.awssdk:s3tables:2.29.26,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.5",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog": catalog_name,
    f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{catalog_name}.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    f"spark.sql.catalog.{catalog_name}.warehouse": table_bucket_arn,
    "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
}


