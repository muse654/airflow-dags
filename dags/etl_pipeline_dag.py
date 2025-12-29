from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl_applemango_mariadb import AppleMangoMariaDBETL
from etl_applemango_opensearch import AppleMangoOpenSearchETL
from etl_guava_mariadb import GuavaMariaDBETL
from etl_guava_clickhouse import GuavaClickHouseETL


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "clickhouse"],
) as dag:

    def applemango_mariadb_etl():
        etl = AppleMangoMariaDBETL(
            table_name="example_table",
            primary_key="id",
        )
        etl.run()

    def applemango_opensearch_etl():
        etl = AppleMangoOpenSearchETL(
            index_pattern="perfhist-*",
        )
        etl.run()

    def guava_mariadb_etl():
        etl = GuavaMariaDBETL(
            table_name="example_table",
            primary_key="id",
        )
        etl.run()

    def guava_clickhouse_etl():
        etl = GuavaClickHouseETL(
            source_table="example_table",
        )
        etl.run()

    t1 = PythonOperator(
        task_id="applemango_mariadb",
        python_callable=applemango_mariadb_etl,
    )

    t2 = PythonOperator(
        task_id="applemango_opensearch",
        python_callable=applemango_opensearch_etl,
    )

    t3 = PythonOperator(
        task_id="guava_mariadb",
        python_callable=guava_mariadb_etl,
    )

    t4 = PythonOperator(
        task_id="guava_clickhouse",
        python_callable=guava_clickhouse_etl,
    )

    t1 >> t2
    t3 >> t4
