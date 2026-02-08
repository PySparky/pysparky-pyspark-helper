import pyspark
from pyspark.sql import SparkSession

from pysparky import spark_ext as se

print(pyspark.__version__)

spark = SparkSession.builder.getOrCreate()

data_dict = {"key1": [1, 2, 3], "key2": [3]}
column_names = ["keys", "values"]
df = se.convert_dict_to_dataframe(spark, data_dict, column_names, explode=True)
df.show()
# key1,1
# key2,2
