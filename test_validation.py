# validar_schema.py verifcando el schema del CLIENTE GLUE
import boto3
from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq



def comprobacion_schema():
    glue_client = boto3.client('glue', region_name='us-east-1')
    lista_glue =[]

    try:
        datos_silver = glue_client.get_table(
            DatabaseName='nyc_tlc_db',
            Name='datos_silver'
        )
        schema_datos = datos_silver['Table']['StorageDescriptor']['Columns']
        # print(f'cantidad columnas: {len(schema_datos)}')

        for i in schema_datos:
            lista_glue.append(i['Name'].lower())

        return lista_glue
        # print(schema_datos)
    except ClientError as e:
        print(f"Error: {e.response['Error']['Message']}")


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
            print(f"Error: {e.response['Error']['Message']}")

    

lista_glue = comprobacion_schema()
lista_s3 = comprobacion_schema_s3()

def comparacion_listas():
    if lista_glue == lista_s3:
        print('el schema es igual')
    else:
        raise Exception("Error: el schema de Glue no coincide con el schema de S3")


if __name__ == "__main__":
    comprobacion_schema()
    comprobacion_schema_s3()
    comparacion_listas()
    