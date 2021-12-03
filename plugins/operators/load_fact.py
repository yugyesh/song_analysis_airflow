from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = "#F98866"

    loading_query = """
    INSERT INTO {}(
        {}
    )
    {}
    """

    @apply_defaults
    def __init__(
        self,
        *args,
        redshift_conn_id="",
        table_name="",
        column_names="",
        selecting_query="",
        **kwargs,
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.selecting_query = selecting_query
        self.table_name = table_name
        self.column_names = column_names

    def execute(self, context):
        """This operator allows task to insert data from the staging tables

        Args:
            context (context): context of the operator
        """
        self.log.info(f"Loading data to the dimension table {self.table_name}")

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from {self.table_name} table")
        redshift_hook.run("TRUNCATE FROM {}".format(self.table_name))

        self.log.info(f"Inserting data into the {self.table_name}")  # try:
        formated_query = LoadFactOperator.loading_query.format(
            self.table_name, self.column_names, self.selecting_query
        )
        redshift_hook.run(formated_query)
