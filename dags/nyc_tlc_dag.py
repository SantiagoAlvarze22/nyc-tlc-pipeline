from datetime import datetime
from airflow.sdk import DAG, task
from tasks.validation import comprobacion_schema,comprobacion_schema_s3
from tasks.catalog import crawler_automatico


def comparacion_listas():
    lista_glue = comprobacion_schema()
    lista_s3 = comprobacion_schema_s3()
    if lista_glue == lista_s3:
        print('el schema es igual')
    else:
        raise Exception("Error: el schema de Glue no coincide con el schema de S3")


with DAG(
    "Pipeline_NYC_TLC",
    description="Pipeline y orquetación de NYC-TLC",
    schedule=None, 
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

    @task
    def comparacion():
        comparacion_listas()

    t3 = comparacion()
         
    @task
    def crawler_funcionando():
        crawler_automatico()
  

    t4 = crawler_funcionando()
    

    t1 >> t2 >> t3 >> t4