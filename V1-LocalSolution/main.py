import os
import glob
import csv
import time
import mysql.connector as mysql
import datetime
from pprint import pprint
import logging
from decouple import config


logging.basicConfig(filename='./logfile',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger()


def drop_tables_database():
    logger.info('Borrando tablas antiguas de la base de datos')
    db = mysql.connect(
        host=config('MYSQL_HOST'),
        user=config('MYSQL_USER'),
        passwd=config('MYSQL_PASSWORD')
    )
    try:
        cursor = db.cursor()
        cursor.execute("DROP DATABASE IF EXISTS pragma")
    except Exception as e:
        logger.error(f'Error al borrar DB antiguas: {e}')
    return


def create_database():
    logger.info('Creando la base de datos')
    db = mysql.connect(
        host=config('MYSQL_HOST'),
        user=config('MYSQL_USER'),
        passwd=config('MYSQL_PASSWORD')
    )
    try:
        cursor = db.cursor()
        cursor.execute(
            f"""CREATE DATABASE IF NOT EXISTS {config('MYSQL_DATABASE')}""")
    except Exception as e:
        logger.error(f'Error creando DB: {e}')

    return


def create_tables_database():
    logger.info('Creando tabla en la base de datos')
    create_database()
    db = mysql.connect(
        host=config('MYSQL_HOST'),
        user=config('MYSQL_USER'),
        passwd=config('MYSQL_PASSWORD'),
        database=config('MYSQL_DATABASE')
    )

    try:
        cursor = db.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS data (
          timestamp DATE, 
          price INT(11),
          user_id INT(11),
          insert_date DATETIME,
          source_file VARCHAR(255)
          )""")

        cursor.execute(
            """CREATE TABLE IF NOT EXISTS records (
          source_file VARCHAR(255),
          insert_date DATETIME
          )""")
    except Exception as e:
        logger.error(f'Error creando tablas en DB: {e}')

    return


def read_files_names_local(path='./', pattern='2012*.csv'):
    logger.info(f'Leyendo archivos coincidentes con {pattern} en {path}')
    os.chdir(path)
    return glob.glob(pattern)


def clean_date_field(date_input):
    try:
        mes, dia, agno = list(map(lambda x: int(x), date_input.split('/')))
        x = datetime.datetime(agno, mes, dia)
        return x.strftime("%Y-%m-%d")
    except Exception as e:
        logger.error(f"Error Limpiando fecha: {e}")
        return


def insert_database(date, user_id, file, actual_price=None):
    db = mysql.connect(
        host=config('MYSQL_HOST'),
        user=config('MYSQL_USER'),
        passwd=config('MYSQL_PASSWORD'),
        database=config('MYSQL_DATABASE')
    )
    try:
        logger.info(f"Insertando en DB: date: {date}, price: {actual_price}, user_id: {user_id}")
        cursor = db.cursor()
        if actual_price:
            query = "INSERT INTO data (timestamp, price, user_id, insert_date, source_file) VALUES (%s, %s, %s, %s, %s)"
            values = (date,
                      actual_price,
                      user_id,
                      time.strftime('%Y-%m-%d %H:%M:%S'),
                      file)
            cursor.execute(query, values)
            db.commit()
        else:
            query = "INSERT INTO data (timestamp, user_id, insert_date, source_file) VALUES (%s, %s, %s, %s)"
            values = (date,
                      user_id,
                      time.strftime('%Y-%m-%d %H:%M:%S'),
                      file)
            cursor.execute(query, values)
            db.commit()
    except Exception as e:
        logger.error(f'Error al insertar datos del archivo {file} en DB: {e}')
    return


def insert_record(file):
    db = mysql.connect(
        host=config('MYSQL_HOST'),
        user=config('MYSQL_USER'),
        passwd=config('MYSQL_PASSWORD'),
        database=config('MYSQL_DATABASE')
    )
    try:
        logger.info(f"Insertando en DB registro del archivo: {file}")
        cursor = db.cursor()
        query = "INSERT INTO records (source_file, insert_date) VALUES (%s, %s)"
        values = (file,
                  time.strftime('%Y-%m-%d %H:%M:%S'))
        cursor.execute(query, values)
        db.commit()

    except Exception as e:
        logger.error(f'Error al insertar registro: {e}')
    return


def read_values_db():
    db = mysql.connect(
        host=config('MYSQL_HOST'),
        user=config('MYSQL_USER'),
        passwd=config('MYSQL_PASSWORD'),
        database=config('MYSQL_DATABASE')
    )

    logger.info('Consultando resultados en base de datos')
    try:
        cursor = db.cursor()
        query = f"SELECT COUNT(*) FROM data"
        cursor.execute(query)
        number_results = cursor.fetchone()[0]

        query = f"SELECT MAX(price) FROM data"
        cursor.execute(query)
        max_price = cursor.fetchone()[0]

        query = f"SELECT MIN(price) FROM data"
        cursor.execute(query)
        min_price = cursor.fetchone()[0]

        query = f"SELECT AVG(price) FROM data"
        cursor.execute(query)
        avg_price = float(cursor.fetchone()[0])

        return {'Número de resultados': number_results,
                'Valor máximo': max_price,
                'Valor mínimo': min_price,
                'Valor Promedio': avg_price}

    except Exception as e:
        logger.error(f'Error al consultar DB: {e}')
        return


def update_indicators(actual_price, line_count=0, price_count=0, price_sum=0, price_min=0, price_max=0):
    try:
        price_sum += actual_price
        price_count += 1

        if line_count == 0:
            price_min = actual_price
            price_max = actual_price

        elif actual_price < price_min:
            price_min = actual_price

        elif actual_price > price_max:
            price_max = actual_price

        price_avg = price_sum / price_count
        return actual_price, line_count, price_count, price_sum, price_min, price_max, price_avg

    except Exception as e:
        logger.error(f'Error actualizando indicadores: {e}')
        return


def processed_csv_file(file, line_count=0, price_count=0, price_sum=0, price_min=0, price_max=0):
    with open(file, mode='r') as csv_file:
        logger.info(f'Procesando archivo {file}')
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            date = clean_date_field(row["timestamp"])
            try:
                actual_price = int(row["price"])
                if line_count == 0:
                    actual_price, line_count, price_count, price_sum, price_min, price_max, price_avg = update_indicators(
                        actual_price)
                    insert_database(date, int(row["user_id"]), file, actual_price)
                    line_count += 1
                else:
                    actual_price, line_count, price_count, price_sum, price_min, price_max, price_avg = update_indicators(
                        actual_price, line_count, price_count, price_sum, price_min, price_max)
                    insert_database(date, int(row["user_id"]), file, actual_price)
                    line_count += 1

            except ValueError:
                logger.info(f'El archivo {file} tiene el campo price vacio en la linea {line_count}')
                insert_database(date=date, user_id=int(row["user_id"]), file=file)
                line_count += 1

            except Exception as e:
                logger.error(f'Error en archivo {file}: {e}')

            print(
                f"""\rArchivo actual: {file} \tLineas procesadas: {line_count} \tPromedio: {'{:.4f}'.format(price_avg)} \tMáximo: {price_max} \tMínimo: {price_min}""",
                end='')
            time.sleep(0.2)

    return line_count, price_count, price_sum, price_min, price_max


def main_process(validate=False, line_count=0, price_count=0, price_sum=0, price_min=0, price_max=0):
    if not validate:
        drop_tables_database()
        create_tables_database()
        logger.info('Leyendo archivos csv en el directorio actual...')
        data_files = read_files_names_local()

        for file in data_files:
            logger.info(f'Cargando archivo {file} en DB')
            line_count, price_count, price_sum, price_min, price_max = \
                processed_csv_file(file, line_count, price_count, price_sum, price_min, price_max)
            insert_record(file)

    else:
        logger.info('Leyendo archivo de validacion')
        data_files = read_files_names_local(pattern='validation.csv')
        for file in data_files:
            logger.info(f'Cargando archivo {file} en DB')
            line_count, price_count, price_sum, price_min, price_max = \
                processed_csv_file(file, line_count, price_count, price_sum, price_min, price_max)
            insert_record(file)

    return line_count, price_count, price_sum, price_min, price_max


def run():
    print('Iniciando proceso de carga en DB')
    line_count, price_count, price_sum, price_min, price_max = main_process()
    print()
    print('Validación de resultados. Resultados obtenidos desde DB:')
    pprint(read_values_db())
    print('\n\n')
    print('Insersión de datos de validación:')
    main_process(True, line_count, price_count, price_sum, price_min, price_max)
    print()
    print('Validación de resultados. Resultados obtenidos desde DB:')
    pprint(read_values_db())
    return


run()
