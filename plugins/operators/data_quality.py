from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        fact_table="",
        dim_tables=[],
        staging_table="",
        check_table="",
        check_column="",
        *args,
        **kwargs,
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.dim_tables = dim_tables
        self.check_table = check_table
        self.check_column = check_column
        self.staging_table = staging_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Check that none of the tables are empty
        all_tables = self.dim_tables.copy()
        all_tables.append(self.fact_table)
        for table in all_tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(
                f"Data quality on table {table} check passed with {records[0][0]} records"
            )

        # Check that all of the data are loaded
        # e.g. check that all of the users are loaded
        if self.check_column != "" and self.check_table != "":
            count_data_in_staging = redshift_hook.get_records(
                f"SELECT count({self.check_column}) FROM {self.staging_table}"
            )

            count_data_in_dim = redshift_hook.get_records(
                f"SELECT COUNT({self.check_column} FROM {self.check_table})"
            )

            if count_data_in_dim == count_data_in_staging:
                self.log.info(f"Data has been loaded correctly {self.check_table}")
            else:
                self.log.error(f"Data were lost while loading {self.check_table}")
