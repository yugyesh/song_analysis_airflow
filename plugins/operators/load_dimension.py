from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = "#80BD9E"

    loading_query = """
    INSERT INTO {}(
        {}
    )
    {}
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table_name="",
        column_names="",
        selecting_query="",
        *args,
        **kwargs,
    ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.selecting_query = selecting_query
        self.table_name = (table_name,)
        self.column_names = column_names

    def execute(self, context):
        self.log.info(f"Loading data to the dimension table {self.table_name}")

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from {self.table_name} table")

        # try:
        redshift_hook.run("DELETE FROM {}".format(self.table_name))
        # except Exception as error:
        #     self.log.error(
        #         f"Error while clearing data from {self.table_name} table with error:{error}"
        #     )

        self.log.info(f"Inserting data into the {self.table_name}")

        # try:
        formated_query = LoadDimensionOperator.loading_query.format(
            self.table_name, self.column_names, self.selecting_query
        )
        redshift_hook.run(formated_query)
        # except Exception as error:
        #     self.log.error(
        #         f"Error while loading the data from {self.table_name} table due to:{error}"
        #     )
