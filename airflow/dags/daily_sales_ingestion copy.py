from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "devops",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SPARK_IMAGE = "apache/spark-py:latest"
DOCKER_NETWORK = "infra_default"
SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3"

with DAG(
    dag_id="daily_sales_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=default_args,
    tags=["ndai", "batch", "sales"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo '[daily_sales_ingestion] start'",
    )

    run_sales_job = BashOperator(
        task_id="run_sales_job_spark",
        bash_command=(
            "docker run --rm --network {net} "
            "-v /opt/airflow/repo:/app "
            "{img} /opt/spark/bin/spark-submit "
            "--packages {pkgs} "
            "/app/services/processing/sales_job.py"
        ).format(net=DOCKER_NETWORK, img=SPARK_IMAGE, pkgs=SPARK_PACKAGES),

        #bash_command=(
        #    'docker run --rm --network infra_default \
        #    -v /opt/airflow/repo:/app \
        #    -v ivy_sales:/tmp/.ivy2 \
        #    apache/spark-py:latest bash -lc "mkdir -p /tmp/.ivy2 && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 /app/services/processing/sales_job.py"'
        #).format(net=DOCKER_NETWORK, img=SPARK_IMAGE, pkgs=SPARK_PACKAGES),
    )

    validate_sales = BashOperator(
        task_id="validate_sales_count",
        bash_command=(
            "docker exec -i infra-postgres-1 "
            "psql -U ndai -d ndai -c \"SELECT COUNT(*) FROM sales;\""
        ),
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo '[daily_sales_ingestion] end'",
    )
