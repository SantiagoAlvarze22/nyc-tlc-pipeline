# Pipeline de datos para análisis de taxis en NYC
Usa datos de viajes de taxis amarillos de NYC (2023-2024) del NYC TLC.

## Arquitectura
```
Airflow (orquestador):  
   CloudFront → S3 bronze → S3 silver → S3 gold
                              ↓
                         Glue (catálogo)
                              ↓
                         Athena (SQL)
```

## Stack Tecnológico

- Python 
- AWS S3
- AWS Glue
- AWS Athena 
- Apache Airflow
- Docker

## Estructura del proyecto

```zsh
NYC-TLC  
├── config/  
│   └── airflow.cfg  
├── dags/  
│   └── nyc_tlc_dag.py #Archivo dag con orden de ejecuciones  
├── notebooks/  
│   └── lectura_datos.ipynb  #Notebook EDA  
├── pipelines/  
│   ├── descargar_datos.py  #Obtención de datos
│   └── transformar_silver.py  #Transformación de datos a capa silver
├── plugins/  
├── docker-compose.yaml  #Configuración docker  
└── README.md
```

## Requisitos previos 
1. Tener instalado docker
2. Credenciales AWS configuradas 

## Guía de instalación


```zsh
docker compose up airflow-init
docker compose up -d
```

## Comandos disponibles

Una vez instalado, se pueden utilizar los siguiente comandos:

```zsh
docker compose start    # Iniciar el ambiente de desarrollo
docker compose stop     # Detener el ambiente de desarrollo
docker compose down     # Detener y eliminar el ambiente de desarrollo.
```