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
    anos=[2023, 2024]
    meses = ['01','02','03','04','05','06','07','08','09','10','11','12']
    mi_bucket = 'datalake-nyc-tlc'


    #Schema de referencia
    ref_rut_ene = f's3://{mi_bucket}/datos-silver/year=2023/month=01/yellow_tripdata_2023-01.parquet'
    ref_ene_sch= pq.read_schema(ref_rut_ene)
    lista_s3_ref = [i.name.lower() for i in ref_ene_sch]
    

    #Se recorre para obtener los schemas de cada particion
    for ano in anos:
         for mes in meses:

            #No incluye los valores de enero 2023 los salta 
            if ano == '2023' and mes == '01':
                continue

            ruta = f's3://{mi_bucket}/datos-silver/year={ano}/month={mes}/yellow_tripdata_{ano}-{mes}.parquet'
            schema_s3 = pq.read_schema(ruta)

            # print(type(schema_s3))
            try:
                esquema = pa.schema(schema_s3)

                #Se crea lista para comparar mes a mes, sin necesidad de añadir todos los meses en una sola lista
                lista_mes=[i.name.lower() for i in esquema]

                #comparando schemas
                if lista_mes == lista_s3_ref:
                    print('los schemas son iguales, continua con la validacion')
                    continue
                else:
                    print(f'{lista_mes}, sch 2: \n{lista_s3_ref}')
                    logger.error(f'Fallo en la operación: {e}', exc_info=True)
                    raise ValueError('Los Schemas no coinciden')

            except ClientError as e:
                    print(f"Error al obtener la tabla: {e.response['Error']['Message']}")
                    raise e
            
            except Exception as e:
                print(f"Error: {e.response['Error']['Message']}")
                logger.error(f'Fallo en la operación: {e}', exc_info=True)
                        
                #Para que la tarea se lance como error y marque FAILED    
                raise e
            
     #si pasaron todas las validaciones se retorna la lista de referencia
    return lista_s3_ref

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