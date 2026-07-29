from datetime import datetime, timedelta
from airflow.sdk import DAG, task

with DAG(
    "Pipeline_NYC_TLC",
    default_args={

    },
    description="Pipeline y orquetación de NYC-TLC",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 7, 29),
    catchup=False,
    tags=['nyc-tlc']
) as dag:

    @task.bash
    def descargar_datos() -> str:
        return "python /opt/airflow/pipelines/descargar_datos.py"

    t1 = descargar_datos()

    @task.bash
    def transformar_silver() -> str:
        return "python /opt/airflow/pipelines/transformar_silver.py"

    t2 = transformar_silver()

    t1 >> t2