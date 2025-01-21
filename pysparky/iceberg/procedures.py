# CALL catalog_name.system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1);


class IcebergProcedures:
    sql = None

    def __init__(self, spark, catalog_name):
        self.spark = spark
        self.catalog_name = catalog_name

    def _to_call_procedure_sql(self, procedure_name, **kwargs) -> str:
        """
        Call a procedure in a catalog.

        Parameters
        ----------
        procedure_name : str
            The name of the procedure.
        kwargs : dict
            The arguments for the procedure.

        Returns
        -------
        None
        """
        # filter the kwargs to remove None values
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        return f"CALL {self.catalog_name}.system.{procedure_name}({', '.join([f'{k} => {v}' for k, v in kwargs.items()])});"

    def run_sql(self):
        """
        Run the SQL statement.

        Returns
        -------
        None
        """
        if self.sql is not None:
            print(self.sql)
            # self.spark.sql(self.sql)
        else:
            raise ValueError("SQL statement is not set.")

    def rollback_to_snapshot(self, table, snapshot_id):
        """
        Rollback a table to a specific snapshot ID.

        Parameters
        ----------
        table : str
            The name of the table.
        snapshot_id : int
            The snapshot ID.

        Returns
        -------
        None
        """
        self.sql = self._to_call_procedure_sql(
            "rollback_to_snapshot", table=table, snapshot_id=snapshot_id
        )
        return self

    def rollback_to_timestamp(self, table, timestamp):
        """
        Rollback a table to a specific timestamp.

        Parameters
        ----------
        table : str
            The name of the table.
        timestamp : str
            The timestamp.

        Returns
        -------
        None
        """
        self.sql = self._to_call_procedure_sql(
            "rollback_to_timestamp", table=table, timestamp=timestamp
        )
        return self

    def set_current_snapshot(self, table, snapshot_id=None, ref=None):
        """
        Set the current snapshot of a table.
        Either snapshot_id or ref must be provided but not both.

        Parameters
        ----------
        table : str
            The name of the table.
        snapshot_id : int
            The snapshot ID.
        ref : str
            The reference.

        Returns
        -------
        None
        """
        # Either snapshot_id or ref must be provided but not both.
        assert (snapshot_id is not None and ref is None) or (
            snapshot_id is None and ref is not None
        )
        self.sql = self._to_call_procedure_sql(
            "set_current_snapshot", table=table, snapshot_id=snapshot_id, ref=ref
        )
        return self

    def cherrypick_snapshot(self, table, snapshot_id):
        """
        Cherry-pick a snapshot of a table.

        Parameters
        ----------
        table : str
            The name of the table.
        snapshot_id : int
            The snapshot ID.

        Returns
        -------
        None
        """
        self.sql = self._to_call_procedure_sql(
            "cherrypick_snapshot", table=table, snapshot_id=snapshot_id
        )
        return self

    def publish_changes(self, table, wap_id):
        """
        Publish changes of a table.

        Parameters
        ----------
        table : str
            The name of the table.
        wap_id : int
            The WAP ID.

        Returns
        -------
        None
        """
        self.sql = self._to_call_procedure_sql(
            "publish_changes", table=table, wap_id=wap_id
        )
        return self

    def fast_forward(self, table, branch, to):
        """
        Fast forward a table.

        Parameters
        ----------
        table : str
            The name of the table.
        branch : str
            The branch.
        to : str
            The to.

        Returns
        -------
        None
        """
        self.sql = self._to_call_procedure_sql(
            "fast_forward", table=table, branch=branch, to=to
        )
        return self


# if __name__ == "__main__":
#     # IcebergProcedures("spark", "catalog_name").rollback_to_snapshot("db.sample", 1).run_sql()
#     # IcebergProcedures("spark", "catalog_name").rollback_to_snapshot("db.sample", 1).run_sql()
#     # IcebergProcedures("spark", "catalog_name").rollback_to_timestamp("db.sample", 1).run_sql()
#     # IcebergProcedures("spark", "catalog_name").set_current_snapshot("db.sample", 1).run_sql()
