import pytest
from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType
from transformers import AutoModelForCausalLM, AutoTokenizer

from pysparky.functions.ai.llm import build_text_generation_udf


@pytest.fixture(scope="session")
def real_model_and_tokenizer():
    model_name = "sshleifer/tiny-gpt2"  # Very small for test; ~5MB
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForCausalLM.from_pretrained(model_name)
    return model, tokenizer


@pytest.fixture
def broadcast_model_and_tokenizer(spark, real_model_and_tokenizer):
    model, tokenizer = real_model_and_tokenizer
    sc = spark.sparkContext
    bc_model = sc.broadcast(model)
    bc_tokenizer = sc.broadcast(tokenizer)
    return bc_model, bc_tokenizer


def test_real_sentiment_udf(spark, broadcast_model_and_tokenizer):
    bc_model, bc_tokenizer = broadcast_model_and_tokenizer

    sentiment_udf = build_text_generation_udf(
        bc_model, bc_tokenizer, "What is the sentiment of the following text?"
    )

    schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("sentence", StringType(), nullable=False),
        ]
    )
    data = [
        Row(1, "I love this product."),
        Row(2, "This is the worst experience."),
    ]
    df = spark.createDataFrame(data, schema=schema)

    result_df = df.withColumn("output_column", sentiment_udf("sentence"))
    results = result_df.collect()

    for row in results:
        print(f"Input: {row['sentence']} => Output: {row['output_column']}")
        assert isinstance(row["output_column"], str)
