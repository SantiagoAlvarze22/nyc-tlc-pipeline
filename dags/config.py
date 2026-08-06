import logging
import boto3 

logger = logging.getLogger("airflow.task")
glue_client = boto3.client('glue', region_name='us-east-1')