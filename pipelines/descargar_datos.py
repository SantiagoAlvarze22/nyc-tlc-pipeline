import requests
import boto3 
#Ingesta (ClaudFront -> S3 Bronze/crudo)
import logging

# Configurar el logger estándar de Airflow
logger = logging.getLogger("airflow.task")

anos=['2023', '2024']
meses = ['01','02','03','04','05','06','07','08','09','10','11','12']
mi_bucket = 'datalake-nyc-tlc'

# Inicializar el cliente S3 (detectará las credenciales de aws configure automáticamente)
s3_client = boto3.client('s3')

#Obtengo los datos del año 2023

for ano in anos:
    for mes in meses:
        try:
            url_archivo = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ano}-{mes}.parquet"
            nombre_local = f"yellow_tripdata_{ano}-{mes}.parquet"
            ruta_s3 = f"datos-crudos/year={ano}/month={mes}/{nombre_local}" #Ruta destino s3 para publicar lo archivos
            respuesta = requests.get(url_archivo, stream=True)
            respuesta.raise_for_status()

            s3_client.upload_fileobj(respuesta.raw, mi_bucket, ruta_s3) #subir los archivos a mi bucket en la ruta deseada
            print(f'años {ano}, mes {mes} subidos')
            # with open(nombre_local, 'wb') as archivo: --> Descarga y guarda de forma local
            #     for bloque in respuesta.iter_content(chunk_size=8192):
            #         archivo.write(bloque)
        except requests.exceptions.HTTPError as e_http:
            if e_http.response.status_code == 404:
                print(f"El archivo para el   año {ano} y mes {mes} no existe en el origen (404).")
                logger.error(f'Fallo en la operación: {e_http}', exc_info=True)

            #Para que la tarea se lance como error y marque FAILED    
                raise e_http
            else:
                print(f"Error HTTP al descargar {ano}-{mes}: {e_http}")
                raise e_http
        except Exception as e:
            print(f"Error general en la transferencia {ano}-{mes}: {e}")
            raise e
        