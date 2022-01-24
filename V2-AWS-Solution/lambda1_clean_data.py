import json
import boto3
import urllib
import io
import pandas as pd
import logging
import os


s3 = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def clean_date(df, column_name):
    df[column_name] = df[column_name].astype('datetime64[ns]')
    df[column_name] = df[column_name].dt.strftime("%Y-%m-%d")

    return df


def define_types(df, column_name, type):
    df[column_name] = df[column_name].astype(type, errors='ignore')
    return df


def get_df_object(key, bucket):
    logger.info(f'Obteniendo datos desde bucket: \t{bucket} \t{key}')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df
    except Exception as e:
        logger.error(f'No se pudo leer el archivo: {e}')


def put_csv_object(key, bucket, df):
    logger.info(f'Copiando archivo a la ruta {key}')
    try:
        file = io.BytesIO()
        df.to_csv(file, index=False)
        s3.put_object(Body=file.getvalue(), Bucket=bucket, Key=key)
        return {
            'statusCode': 200,
            'body': json.dumps('El archivo se copio con exito')
        }
    except Exception as e:
        logger.error(f'No se pudo copiar el archivo: {e}')
        return {
            'statusCode': 400,
            'body': json.dumps('No se pudo copiar el archivo')
        }


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    df_data = get_df_object(key, bucket)

    bucket_put = os.environ['BUCKET_PUT']
    key_put = 'Staging/' + key
    put_csv_object(key_put, bucket_put, df_data)

    return {
        'statusCode': 200,
        'body': json.dumps('El archivo se limpio con exito')
    }


"""f = open('event.json')
event = json.load(f)
print(lambda_handler(event, ''))"""
