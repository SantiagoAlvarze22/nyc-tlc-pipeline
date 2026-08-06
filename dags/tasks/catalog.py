from config import glue_client, logger 
from botocore.exceptions import ClientError
from time import sleep

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