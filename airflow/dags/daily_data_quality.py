from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "devops",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="daily_data_quality",
    start_date=datetime(2025, 1, 1),
    schedule="30 2 * * *",  # 02:30
    catchup=False,
    default_args=default_args,
    tags=["ndai", "dq", "quality"],
) as dag:

    # Volumes minimum attendus
    check_counts = BashOperator(
        task_id="check_counts",
        bash_command=(
            "docker exec -i infra-postgres-1 psql -U ndai -d ndai -v ON_ERROR_STOP=1 -c "
            "\"SELECT "
            "(SELECT COUNT(*) FROM sales) AS sales_count, "
            "(SELECT COUNT(*) FROM campaign_feedback_enriched) AS feedback_count;\""
        ),
    )

    # Règles qualité simples 
    check_rules = BashOperator(
        task_id="check_rules",
        bash_command=(
            "docker exec -i infra-postgres-1 psql -U ndai -d ndai -v ON_ERROR_STOP=1 -c "
            "\""
            "SELECT COUNT(*) AS null_sale_date FROM sales WHERE sale_date IS NULL; "
            "SELECT COUNT(*) AS bad_quantity FROM sales WHERE quantity <= 0; "
            "SELECT COUNT(*) AS null_feedback_date FROM campaign_feedback_enriched WHERE feedback_date IS NULL; "
            "SELECT COUNT(*) AS null_comment FROM campaign_feedback_enriched WHERE comment IS NULL OR comment = ''; "
            "\""
        ),
    )