from config import glue_client, logger 
from botocore.exceptions import ClientError
import pyarrow.parquet as pq
import pyarrow as pa

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
                    logger.error(f'Fallo en la operación: schema año{ano}, mes {mes}')
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