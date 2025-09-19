from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# دالة البايثون
def my_python_task():
    print("Hello from Python task in Airflow!")

# تعريف الـ DAG
with DAG(
    dag_id="example_simple_dag",
    default_args={
        "owner": "nouran",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="My first example DAG",
    schedule_interval="@daily",  # تم التعديل من timedelta إلى string
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # مهمة Bash بداية DAG
    start = BashOperator(
        task_id="start",
        bash_command="echo 'Start DAG'"
    )

    # مهمة Python
    python_task = PythonOperator(
        task_id="run_python_task",
        python_callable=my_python_task
    )

    # مهمة Bash نهاية DAG
    end = BashOperator(
        task_id="end",
        bash_command="echo 'End DAG'"
    )

    # ترتيب المهام
    start >> python_task >> end
