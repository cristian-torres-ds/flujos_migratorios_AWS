import json
import boto3
import requests
import pandas as pd
from io import StringIO
from io import BytesIO


# Configura las credenciales de AWS
aws_access_key = '*****'
aws_secret_key = '*****'
region_name = 'us-east-1'  # Cambia esto a tu región

# Configura el nombre del bucket de S3 y la ruta dentro del bucket
bucket_name = 'data-migraciones'

urls = ['https://www.knomad.org/sites/default/files/2023-06/remittance_inflows_brief_38_june_2023_3.xlsx',
        'https://api.worldbank.org/v2/en/indicator/NY.GDP.MKTP.CD?downloadformat=excel',
        'https://api.worldbank.org/v2/en/indicator/FP.CPI.TOTL.ZG?downloadformat=excel',
        'https://api.worldbank.org/v2/en/indicator/SM.POP.NETM?downloadformat=excel'
        ]

keys = ['data/remesas_raw.xlsx',
        'data/pib_raw.xls',
        'data/inflacion_raw.xls',
        'data/migraciones_raw.xls',
        ]

def lambda_handler(event, context):
    for i, url in enumerate(urls):
        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region_name)
    
        # Descargar el archivo CSV desde la URL
        response = requests.get(url)
    
        if response.status_code == 200:
            # Subir el archivo CSV al bucket de S3
            objeto_antiguo = s3_client.get_object(Bucket=bucket_name, Key=keys[i])
            data_antigua = objeto_antiguo['Body'].read()
            df_antiguo = pd.read_excel(BytesIO(data_antigua))

            data_nueva = s3_client.get_object(Body=response.content)
            df_nuevo = pd.read_excel(BytesIO(data_nueva))

            if df_nuevo.equals(df_antiguo) == False:
                s3_client.put_object(Body=response.content, Bucket=bucket_name, Key=keys[i])
            print("Archivo CSV subido exitosamente a S3")
        else:
            print("Error al descargar el archivo CSV")

# Se ejecuta la función
lambda_handler(None, None)