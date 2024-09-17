from pysparky import decorator
import pyspark
from pyspark.sql import Column, DataFrame

from typing import Callable

@decorator.extension_enabler(pyspark.sql.DataFrame)
def try_merge_or_overwrite(
    target_table_name: str,
    source_sdf: DataFrame,
    merge_condition: Column,
    matched_condition: Column,
    custom_overwrite_function: Callable = None,
):
    """
    Attempts to merge the source DataFrame into the target Delta table. If the merge fails due to an unresolved expression,
    it overwrites the target table with the source DataFrame.

    Parameters:
    target_table_name (str): The name of the target Delta table.
    source_sdf (DataFrame): The source DataFrame to merge or overwrite.
    merge_condition (Column): The condition to match rows between the source and target tables.
    matched_condition (Column): The condition to update matched rows in the target table.
    custom_overwrite_function (Callable, Optional): This is the function to overwrite the original function.
                                            Use it if you got partitioned by or other config.
    Returns:
    None
    """  # noqa: E501

    spark = source_sdf.sparkSession

    try:
        print("Running merge refresh...")

        merge_result = (
            DeltaTable.forName(spark, target_table_name)
            .alias("target")
            .merge(source=source_sdf.alias("source"), condition=merge_condition)
            .whenMatchedUpdateAll(condition=matched_condition)
            .whenNotMatchedInsertAll()  # Insert new rows
            .whenNotMatchedBySourceDelete()  # Delete removed rows
            .execute()
        )

        if merge_result:
            merge_result.display()

    except PySparkException as ex:
        if ex.getErrorClass() in [
            "DELTA_MERGE_UNRESOLVED_EXPRESSION",
            "DELTA_MISSING_DELTA_TABLE",
            "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE",
        ]:
            # [DELTA_MERGE_UNRESOLVED_EXPRESSION] - SQLSTATE: 42601
            # Catched it!!
            print(f"[{ex.getErrorClass()}] Running full refresh...")

            if custom_overwrite_function is None:
                spark.conf.set(
                    "spark.databricks.delta.commitInfo.userMetadata",
                    f"Overwriting the table due to {ex.getErrorClass()}",
                )
                source_sdf.writeTo(target_table_name).createOrReplace()
            else:
                custom_overwrite_function()
        else:
            # Continue raise the error
            raise ex
