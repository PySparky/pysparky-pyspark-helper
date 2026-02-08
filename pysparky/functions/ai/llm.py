try:
    from transformers import PreTrainedModel, PreTrainedTokenizer
except ImportError as e:
    raise ImportError(
        "The 'transformers' library is required for this feature. "
        "Install it with: pip install pysparky[ai]"
    ) from e

from pyspark import Broadcast
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.udf import UserDefinedFunction


def build_text_generation_udf(
    model_bc: Broadcast, tokenizer_bc: Broadcast, system_prompt: str
) -> UserDefinedFunction:
    """Creates a Spark UDF for text generation using a Hugging Face model and tokenizer.

    This function sets up a user-defined function (UDF) that can be used in Spark DataFrames
    to perform text generation. It uses a broadcasted Hugging Face model and tokenizer to
    ensure efficient distribution across Spark workers.

    Args:
        model_bc (Broadcast): Broadcasted Hugging Face model.
        tokenizer_bc (Broadcast): Broadcasted Hugging Face tokenizer.
        system_prompt (str): Prompt to prepend to each input string before generation.

    Returns:
        function: A Spark UDF that takes a string input and returns the generated text.

    Raises:
        TypeError: If `model_bc` or `tokenizer_bc` is not a Broadcast instance.

    Example:
        ```python
        >>> from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
        >>> model = AutoModelForSeq2SeqLM.from_pretrained("google/flan-t5-small")
        >>> tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-small")
        >>> t5_udf = build_text_generation_udf(
        ...     sc.broadcast(model), sc.broadcast(tokenizer), "sentiment of the text"
        ... )
        >>> results_df = input_df.withColumn("output_column", t5_udf("sentence"))
        ```
    """
    if not isinstance(model_bc, Broadcast):
        raise TypeError(
            "model_bc must be a pyspark.Broadcast instance. "
            "Broadcasting ensures the model is efficiently shared across Spark workers."
        )
    if not isinstance(tokenizer_bc, Broadcast):
        raise TypeError(
            "tokenizer_bc must be a pyspark.Broadcast instance. "
            "Broadcasting ensures the tokenizer is efficiently shared across Spark workers."
        )

    model: PreTrainedModel = model_bc.value
    tokenizer: PreTrainedTokenizer = tokenizer_bc.value

    @udf(StringType())
    def generate_text_udf(user_input: str) -> str:
        """Generates text from the model using the given input and system prompt."""
        full_input = f"{system_prompt}: {user_input}"
        inputs = tokenizer(full_input, return_tensors="pt")
        outputs = model.generate(**inputs)
        return tokenizer.decode(outputs[0], skip_special_tokens=True)

    return generate_text_udf
