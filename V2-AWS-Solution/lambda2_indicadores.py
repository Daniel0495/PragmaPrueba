import json
import boto3
import urllib
import io
import pandas as pd
import logging
import os


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def clean_date(df, column_name):
    df[column_name] = df[column_name].astype('datetime64[ns]')
    df[column_name] = df[column_name].dt.strftime("%Y-%m-%d")
    return df


def define_types(df, column_name, type):
    df[column_name] = df[column_name].astype(type, errors='ignore')
    return df


def generate_indicators(df, key):
    logger.info(f'Generando indicadores: Leyendo archivo {key}')
    min_price = df['price'].min()
    max_price = df['price'].max()
    sum_price = df['price'].sum()
    count_price = len(df[df['price'].isnull() == False])

    summary = {'min_price': [min_price],
               'max_price': [max_price],
               'sum_price': [sum_price],
               'count_price': [count_price],
               'avg_price': [float('{:.4}'.format(str(sum_price / count_price)))]
               }

    return pd.DataFrame.from_dict(summary)


def put_parquet_object(key, bucket, df):
    logger.info(f'Copiando archivo a la ruta {key}')
    try:
        file = io.BytesIO()
        df.to_parquet(file, index=False)
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


def get_df_object(key, bucket):
    logger.info(f'Obteniendo datos desde bucket: \t{bucket} \t{key}')
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df
    except Exception as e:
        logger.error(f'No se pudo leer el archivo: {e}')


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    df_data = get_df_object(key, bucket)
    df_data = clean_date(df_data, 'timestamp')
    df_data = define_types(df_data, 'price', float)
    df_data = define_types(df_data, 'user_id', int)

    # bucket_put = 'pruebapragma-data-stage-dev'
    bucket_put = os.environ['BUCKET_PUT']
    file_name = key.split('/')[1][:-4]
    if file_name == 'validation':
        month_key = 6
    else:
        month_key = file_name[5]
    key_put = f'Stage/Data/year=2012/monnth={month_key}/' + file_name + '.parquet'
    put_parquet_object(key_put, bucket_put, df_data)

    df_summary = generate_indicators(df_data, file_name)
    key_put_summary = f'Stage/Conteos/year=2012/monnth={month_key}/' + file_name + '.parquet'
    put_parquet_object(key_put_summary, bucket_put, df_summary)

    return {
        'statusCode': 200,
        'body': json.dumps('Indicadores creados con exito')
    }


"""f = open('event.json')
event = json.load(f)
print(lambda_handler(event, ''))"""
