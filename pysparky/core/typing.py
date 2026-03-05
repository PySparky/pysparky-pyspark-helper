from typing import TypeAlias

from pyspark.sql import Column

ColumnOrName: TypeAlias = Column | str
