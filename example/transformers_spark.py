from pyspark.sql import Row, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

# Spark session
spark = SparkSession.builder.appName("T5 mapPartitions").getOrCreate()
sc = spark.sparkContext

# Sample data
schema = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("sentence", StringType(), nullable=False),
    ]
)

data = [
    Row(1, "It is a good test for Spark."),
    Row(2, "Spark DataFrames are powerful."),
    Row(3, "LLMs could be very slow."),
    Row(4, "It is a naive statement."),
]

df = spark.createDataFrame(data, schema=schema)

# Load and broadcast model/tokenizer
model = AutoModelForSeq2SeqLM.from_pretrained("google/flan-t5-small")
tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-small")
broadcast_model = sc.broadcast(model)
broadcast_tokenizer = sc.broadcast(tokenizer)


# Function to process a partition
def process_partition(rows):
    model = broadcast_model.value
    tokenizer = broadcast_tokenizer.value

    for row in rows:
        prompt = f"sentiment of the text: {row['sentence']}"
        inputs = tokenizer(prompt, return_tensors="pt")
        output = model.generate(**inputs)
        result = tokenizer.decode(output[0], skip_special_tokens=True)

        yield (row["id"], row["sentence"], result)


# Use mapPartitions
result_rdd = df.rdd.mapPartitions(process_partition)

# Convert to DataFrame
result_df = result_rdd.toDF(["id", "sentence", "output_column"])
result_df.show(truncate=False)
