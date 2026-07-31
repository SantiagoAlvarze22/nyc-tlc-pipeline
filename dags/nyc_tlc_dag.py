from datetime import datetime, timedelta
from airflow.sdk import DAG, task
from time import sleep
import boto3 
from botocore.exceptions import ClientError

def crawler_automatico():
    try:
            #Llamar cliente clue
            glue_client = boto3.client('glue', region_name='us-east-1')
            response_get_crawler = glue_client.get_crawler(Name='silver-crawler-2')
            crawler_state = response_get_crawler['Crawler']['State']

            #si está listo para que lo llamen, entonces que inicie el crawler
            if crawler_state == 'READY':
                #Inicia el crawler
                glue_client.start_crawler(
                                        Name='silver-crawler-2'
                                        )
            else:
                print(f"El crawler ya se está ejecutando. Estado actual: {crawler_state}")

            #para que actualice el estado se deja este tiempo
            sleep(10)

            while True:
                #Se llama el estado para ver si actualizó después de iniciar
                response_get_crawler = glue_client.get_crawler(Name='silver-crawler-2')
                crawler_state=response_get_crawler['Crawler']['State']
                if crawler_state == 'READY':
                    #Para dag
                    break  
                elif crawler_state == 'RUNNING' or crawler_state == 'STOPPING':
                     sleep(30)
                     continue
                
    except ClientError as err:
            if err.response['Error']['Code'] == 'EntityNotFoundException':
                print("Error: The specified crawler does not exist.")
            else:
                print(f"Unexpected error: {err}")

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
    def crawler_funcionando():
        crawler_automatico()
  

    t3 = crawler_funcionando()
    

    t1 >> t2 >> t3