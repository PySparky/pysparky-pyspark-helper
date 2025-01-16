aws_athena_spark_config = {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.7.11,org.apache.iceberg:iceberg-aws-bundle:1.7.12,software.amazon.awssdk:url-connection-client:2.27.213,org.apache.hadoop:hadoop-aws:3.4.14",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
    "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg_catalog.glue.id": "123456789012",
    "spark.sql.catalog.iceberg_catalog.warehouse": "s3://<bucket_name>",
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.broadcastTimeout": "3600s",
    "spark.executor.heartbeatInterval": "600s",
    "spark.network.timeout": "700s",
}

aws_s3_spark_config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.1",
}

mongodb_spark_config = {
    "spark.mongodb.read.connection.uri": "<connection_string>",
    "spark.mongodb.write.connection.uri": "<connection_string>",
    "spark.jars.packages": "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
}
