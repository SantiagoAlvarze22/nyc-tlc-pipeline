import pandas as pd 
#Limpienza (s3 bronze/crudo -> s3 Silver)

anos=[2023, 2024]
meses = ['01','02','03','04','05','06','07','08','09','10','11','12']

mi_bucket = 'datalake-nyc-tlc'

#Recorro anos y meses para brindar la descarga y la actualización de los valores 

for ano in anos:
    for mes in meses:
        try:
            #Lectura bucket todos los años
            ruta_s3 = f"s3://datalake-nyc-tlc/datos-crudos/year={ano}/month={mes}/yellow_tripdata_{ano}-{mes}.parquet"
            #se lee el parquet
            df = pd.read_parquet(ruta_s3)

            #Filtros al df trip_distance > 0 y  <= 20.06,
            # fare_amout > 0 y <= 100 
            df_silver = df[(df['trip_distance'] > 0) 
               & (df['trip_distance'] <= 20.06) 
               & (df['fare_amount'] > 0) 
               & (df['fare_amount'] <= 100)]

            df_silver_ren = df_silver.rename(columns={
                "Airport_fee":"airport_fee"
            })


            #Subir en la ruta nueva silver
            nombre_archivo = f"yellow_tripdata_{ano}-{mes}.parquet"
            ruta_s3_silver = f"datos-silver/year={ano}/month={mes}/{nombre_archivo}"

            # Convierte df a parquet 
            df_silver_ren.to_parquet(f"s3://{mi_bucket}/{ruta_s3_silver}", index=False)

            print(f"archivo {nombre_archivo} cargado y actualizado")
        except Exception as e:
            print(f"Error general en la transferencia {ano}-{mes}: {e}")
