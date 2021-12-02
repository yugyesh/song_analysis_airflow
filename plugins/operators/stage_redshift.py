from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    template_fields = ("s3_key",)

    # copy_sql
    staging_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS JSON '{}'
    REGION '{}'
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credential_id="",
        table="",
        s3_bucket="",
        s3_key="",
        *args,
        **kwargs,
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        """
        This method copy's data from the s3 to redshift staging table
        Args:
            context (context): context of the operator
        """

        self.log.info(f"Staging {self.table} table")

        aws_hook = AwsBaseHook("aws_credentials", client_type="redshift")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data form {self.table} table")
        redshift_hook.run(f"DELETE FROM {self.table}")

        self.log.info(f"Copy data from the s3 to {self.table} table in redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.staging_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            Variable.get("json_format"),
            Variable.get("region"),
        )
        redshift_hook.run(formatted_sql)
