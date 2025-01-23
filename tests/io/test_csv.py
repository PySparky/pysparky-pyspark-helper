import os
import tempfile

from pysparky.io.csv import write_single_csv


def test_write_single_csv(spark):
    # Create a sample DataFrame
    sdf = spark.range(1)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, "test_file.csv")

        # Call the function
        write_single_csv(sdf, temp_file_path, header=True, mode="overwrite")

        # Check if the file is created
        assert os.path.isfile(temp_file_path), "CSV file was not created."

        # Check the content of the file
        with open(temp_file_path, "r") as f:
            content = f.read()
            assert "id" in content, "CSV file content is incorrect."
