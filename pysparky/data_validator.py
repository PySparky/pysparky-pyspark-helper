from dataclasses import dataclass, field
from typing import Union

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pysparky import enabler
from pysparky import functions as F_
from pysparky.transformations import filters


@dataclass
class ValidationRule:
    """
    A class to represent a validation rule.

    Attributes:
        name (str): The name of the validation rule.
        conditions (list[Column]): A list of conditions (Spark Columns) that make up the rule.
        combined_condition (Column): The combined condition of all the conditions using logical AND.
            It will generate from conditions

    Example:
        ValidationRule("first_name_check", F_.printable_only("first_name")),
    """

    name: str
    conditions: list[Column] | Column
    combined_condition: Column = field(init=False)

    def __post_init__(self):
        self.conditions = enabler.ensure_list(self.conditions)
        self.combined_condition = F_.condition_and(*self.conditions)


@dataclass
class DataValidator:
    """
    A class to validate data based on a set of validation rules.

    Attributes:
        rules (list[ValidationRule]): A list of validation rules.

    Example:
        ```python
        >>> ValidationRules = [
        ...     ValidationRule("first_name_check", F_.is_printable_only("first_name")),
        ...     ValidationRule("last_name_check", F_.is_printable_only("last_name")),
        ...     ValidationRule("address_check", F_.is_printable_only("address")),
        ...     ValidationRule("region_check", F_.is_printable_only("region")),
        ...     ValidationRule("code_check", [F_.is_two_character_only("code")]),
        ...     ValidationRule("postcode_check", F_.is_printable_only("postcode")),
        ... ]

        >>> validator = DataValidator(ValidationRules)

        >>> conditions = {
        ...     "first_name_check": F_.is_printable_only("first_name"),
        ...     "last_name_check": F_.is_printable_only("last_name"),
        ...     "address_check": F_.is_printable_only("address"),
        ...     "region_check": F_.is_printable_only("region"),
        ...     "code_check": [F_.is_two_character_only("code")],
        ...     "postcode_check": F_.is_printable_only("postcode"),
        ... }

        >>> validator = DataValidator.from_dict(conditions)
        ```
    """

    rules: list[ValidationRule]

    @classmethod
    def from_dict(cls, data: dict[str, Union[list[Column], Column]]) -> "DataValidator":
        """
        Creates a DataValidator instance from a dictionary.

        Args:
            data (dict[str, list[Column] | Column]): A dictionary where keys are rule names and values are lists of conditions or a single condition.

        Returns:
            DataValidator: An instance of DataValidator.

        Example:
            ```python
            >>> conditions = {
            ...     "first_name_check": F_.is_printable_only("first_name"),
            ...     "last_name_check": F_.is_printable_only("last_name"),
            ...     "address_check": F_.is_printable_only("address"),
            ...     "region_check": F_.is_printable_only("region"),
            ...     "code_check": [F_.is_two_character_only("code")],
            ...     "postcode_check": F_.is_printable_only("postcode"),
            ... }
            >>> validator = DataValidator.from_dict(conditions)
            ```

        """
        rules = [ValidationRule(name, conditions) for name, conditions in data.items()]
        return cls(rules=rules)

    @property
    def query_map(self) -> dict[str, Column]:
        """
        Gets a dictionary mapping rule names to their combined conditions.
        The key is the column name and the value is the Column Object.

        Returns:
            dict[str, Column]: A dictionary where keys are rule names
                and values are combined conditions.

        Example:
            ```python
            >>> sdf.withColumns(validator.query_map)
            ```

        """
        return {rule.name: rule.combined_condition for rule in self.rules}

    def apply_conditions(self, sdf: DataFrame) -> DataFrame:
        """
        Applies the combined conditions to the Spark DataFrame.

        Args:
            sdf (DataFrame): The Spark DataFrame to which the conditions will be applied.

        Returns:
            DataFrame: The Spark DataFrame with the conditions applied.

        Example:
            ```python
            >>> validator.apply_conditions(data_sdf)
            ```

        """
        return sdf.withColumns(self.query_map)

    def filter_invalid(self, sdf: DataFrame) -> DataFrame:
        """
        Filters out invalid rows from the Spark DataFrame based on the rules.

        Args:
            sdf (DataFrame): The Spark DataFrame to be filtered.

        Returns:
            DataFrame: The Spark DataFrame with invalid rows filtered out.

        Example:
            ```python
            >>> validator.filter_invalid(data_sdf)
            ```

        """
        return filters(
            self.apply_conditions(sdf),
            [
                (
                    F.col(column_name) == False  # noqa: E712
                )  # pylint: disable=singleton-comparison
                for column_name in self.query_map.keys()
            ],
            operator_="or",
        )

    def filter_valid(self, sdf: DataFrame) -> DataFrame:
        """
        Filters out valid rows from the Spark DataFrame based on the rules.

        Args:
            sdf (DataFrame): The Spark DataFrame to be filtered.

        Returns:
            DataFrame: The Spark DataFrame with valid rows filtered out.

        Example:
            ```python
            >>> validator.filter_valid(data_sdf).select(data_sdf.columns).show()
            ```

        """
        return filters(
            self.apply_conditions(sdf),
            [
                (
                    F.col(column_name) == True  # noqa: E712
                )  # noqa: E712 # pylint: disable=singleton-comparison
                for column_name in self.query_map.keys()
            ],
            operator_="and",
        )
