import glob
import os
import tempfile

from pyspark.sql import DataFrame


def write_single_csv(sdf: DataFrame, path: str, **csvKwargs):
    """
    Writes a single CSV file from a Spark DataFrame.

    Args:
        sdf (DataFrame): The Spark DataFrame to write.
        path (str): The destination path for the CSV file.
        **csvKwargs: Additional keyword arguments to pass to the DataFrame's write.csv method.

    Examples:
        ```python
        >>> write_single_csv(spark.range(1), "temp/file.csv", header=True, mode='overwrite')
        ```
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        sdf.repartition(1).write.csv(temp_dir, **csvKwargs)
        csv_file = glob.glob(os.path.join(temp_dir, "*.csv"))[0]
        os.rename(csv_file, path)


# def write_single_csv_s3(sdf: DataFrame, file_name: str, s3_profile_name: str, s3_prefix: str, **csvKwargs):
#     """
#     Writes a single CSV file from a Spark DataFrame to an S3 bucket.

#     Args:
#         sdf (DataFrame): The Spark DataFrame to write.
#         file_name (str): The name of the CSV file to be created.
#         s3_profile_name (str): The AWS profile name to use for S3 access.
#         s3_prefix (str): The S3 bucket and prefix where the file will be stored.
#         **csvKwargs: Additional keyword arguments to pass to the DataFrame's write.csv method.

#     Examples:
#         ```python
#         >>> write_single_csv_s3(spark.range(1), "file.csv", s3_profile_name="my_profile", s3_prefix="s3://bucket/cenz", header=True, mode='overwrite')
#         ```

#     Note:
#     Only 'overwrite' mode is supported.
#     """
#     import s3fs

#     with tempfile.TemporaryDirectory() as temp_dir:
#         full_temp_file_name = f"{temp_dir}/{file_name}"
#         full_s3_file_name = f"{s3_prefix}/{file_name}"

#         write_single_csv(sdf, full_temp_file_name, **csvKwargs)

#         profile_name = s3_profile_name
#         fs = s3fs.S3FileSystem(profile=profile_name)
#         fs.put(full_temp_file_name, full_s3_file_name)
