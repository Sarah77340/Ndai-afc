from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "ndai",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="daily_sales_ingestion",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Vérifier MinIO
    check_minio = BashOperator(
        task_id="check_minio",
        bash_command="""
        docker run --rm --network infra_default curlimages/curl:8.6.0 -s http://minio:9000/minio/health/ready
        """,
    )

    # Lancer Spark exactement comme en manuel
    #run_spark_sales = BashOperator(
    #    task_id="run_spark_sales",
    #    bash_command="""
    #    docker run --rm --network infra_default \
    #      -v /opt/airflow/repo:/app \
    #      -v /opt/airflow/repo/.ivy2:/tmp/.ivy2 \
    #      apache/spark-py:latest \
    #      /opt/spark/bin/spark-submit \
    #      --conf spark.jars.ivy=/tmp/.ivy2 \
    #      --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
    #      /app/services/processing/sales_job.py
    #    """,
    #)

    #run_spark_sales = BashOperator(
    #    task_id="run_spark_sales",
    #    bash_command=r"""
    #    set -e
    #
    #    # 1) Crée un volume Docker pour Ivy (cache Maven) si besoin
    #    docker volume inspect spark_ivy_cache >/dev/null 2>&1 || docker volume create spark_ivy_cache >/dev/null

    #    # 2) Lance Spark (même commande que ton manuel) avec Ivy en volume
    #    docker run --rm --network infra_default \
    #    -v /opt/airflow/repo:/app \
    #    -v spark_ivy_cache:/tmp/.ivy2 \
    #    apache/spark-py:latest \
    #    /opt/spark/bin/spark-submit \
    #    --conf spark.jars.ivy=/tmp/.ivy2 \
    #    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
    #    /app/services/processing/sales_job.py
    #    """,
    #)

    run_spark_sales = BashOperator(
        task_id="run_spark_sales",
        bash_command=r"""
        set -e

        docker volume inspect spark_ivy_cache >/dev/null 2>&1 || docker volume create spark_ivy_cache

        docker run --rm --network infra_default \
        -v /opt/airflow/repo:/app \
        -v spark_ivy_cache:/tmp/.ivy2 \
        apache/spark-py:latest \
        /opt/spark/bin/spark-submit \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
        /app/services/processing/sales_job.py
        """,
    )
    

    # Vérifier que Postgres a des données
    check_sales = BashOperator(
        task_id="check_sales",
        bash_command="""
        docker exec infra-postgres-1 psql -U ndai -d ndai -c "SELECT COUNT(*) FROM sales;"
        """,
    )
