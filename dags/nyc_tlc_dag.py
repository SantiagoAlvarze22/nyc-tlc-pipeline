from datetime import datetime, timedelta
from airflow.sdk import DAG, task
from time import sleep
import boto3 
from botocore.exceptions import ClientError
import logging
import pyarrow as pa
import pyarrow.parquet as pq

# Configurar el logger estándar de Airflow
logger = logging.getLogger("airflow.task")
 #Llamar cliente clue
glue_client = boto3.client('glue', region_name='us-east-1')

def crawler_automatico():
    try:
            response_get_crawler = glue_client.get_crawler(Name='silver-crawler-2')
            crawler_state = response_get_crawler['Crawler']['State']

            #si está listo para que lo llamen, entonces que inicie el crawler
            if crawler_state == 'READY':
                #Inicia el crawler
                glue_client.start_crawler(Name='silver-crawler-2')
            else:
                print(f"El crawler ya se está ejecutando. Estado actual: {crawler_state}")

            #para que actualice el estado se deja este tiempo
            sleep(10)

            while True:
                #Se llama el estado para ver si actualizó después de iniciar
                response_get_crawler = glue_client.get_crawler(Name='silver-crawler-2')
                crawler_state=response_get_crawler['Crawler']['State']
                if crawler_state == 'READY':
                    #Parar dag
                    break  
                elif crawler_state == 'RUNNING' or crawler_state == 'STOPPING':
                     sleep(30)
                     continue
                
    #Toma la AccessDeniedException si no hay permisos para el usuario desde aws 
    except glue_client.exceptions.AccessDeniedException as e:
        print(f"Access Denied (Specific): {e.response['Error']['Message']}")
        logger.error(f'Fallo en la operación: {e}', exc_info=True)

        #Para que la tarea se lance como error y marque FAILED    
        raise e
            
    except ClientError as err:
            if err.response['Error']['Code'] == 'EntityNotFoundException':
                print("Error: The specified crawler does not exist.")
                raise err
            else:
                print(f"Unexpected error: {err}")
                raise err

def comprobacion_schema():
    lista_glue =[]
    try:
        datos_silver = glue_client.get_table(
             DatabaseName='nyc_tlc_db',
             Name='datos_silver'
        )

        schema_datos = datos_silver['Table']['StorageDescriptor']['Columns']

        #Modiifca nombres a minúscula
        for i in schema_datos:
                    lista_glue.append(i['Name'].lower())

        return lista_glue
                # print(schema_datos)

    except ClientError as e:
        print(f"Error al obtener la tabla: {e.response['Error']['Message']}")

        #Para que la tarea se lance como error y marque FAILED    
        raise e

    except glue_client.exceptions.InvalidInputException as e:
        print(f"Error: {e.response['Error']['Message']}")
        logger.error(f'Fallo en la operación: {e}', exc_info=True)
                
        #Para que la tarea se lance como error y marque FAILED    
        raise e
    

def comprobacion_schema_s3():
    ruta = 's3://datalake-nyc-tlc/datos-silver/year=2023/month=01/yellow_tripdata_2023-01.parquet'
    schema_s3 = pq.read_schema(ruta)
    lista_s3 = []
    # print(type(schema_s3))
    try:
        esquema = pa.schema(schema_s3)
        for campo in esquema:
            lista_s3.append(campo.name.lower())

        return lista_s3

    except ClientError as e:
            print(f"Error al obtener la tabla: {e.response['Error']['Message']}")
            raise e
    
    except Exception as e:
        print(f"Error: {e.response['Error']['Message']}")
        logger.error(f'Fallo en la operación: {e}', exc_info=True)
                
        #Para que la tarea se lance como error y marque FAILED    
        raise e
   
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