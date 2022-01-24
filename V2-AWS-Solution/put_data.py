import json
import time
import boto3
import io
import os
import glob
import logging
import pandas as pd


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def read_files_names_local(path='./', pattern='2012*.csv'):
    logger.info(f'Leyendo archivos coincidentes con {pattern} en {path}')
    os.chdir(path)
    return glob.glob(pattern)


def put_csv_object(key, bucket, df):
    try:
        logger.info(f'Copiando archivo a la ruta {key}')
        file = io.BytesIO()
        df.to_csv(file, index=False)
        s3.put_object(Body=file.getvalue(), Bucket=bucket, Key=key)
        return True
    except Exception as e:
        logger.error(f'No se pudo subir el archivo: {e}')
        return False


def lambda_handler(event, context, name_file=None):
    files = read_files_names_local(pattern='*.csv')
    bucket = 'pruebapragma-data-raw-dev'
    for i, file in enumerate(files):
        print(file)
        df = pd.read_csv(file)
        put_csv_object(file, bucket, df)
        time.sleep(2)
    return True


f = open('event.json')
event = json.load(f)
print(lambda_handler(event, ''))
