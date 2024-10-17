import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pysparky import functions as F_
from pysparky import spark_ext as se
from pysparky import transformations as te

print(pyspark.__version__)

spark = SparkSession.builder.getOrCreate()

data_dict = {"key1": [1, 2, 3], "key2": [3]}
column_names = ["keys", "values"]
df = se.convert_dict_to_dataframe(spark, data_dict, column_names, explode=True)
df.show()
# key1,1
# key2,2
