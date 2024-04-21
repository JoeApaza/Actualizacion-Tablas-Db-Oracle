# Ingresar las librerias a usar
import logging
#import cx_Oracle tambien puede usar esta libreria en reemplazo de oracledb
import oracledb 
import polars as pl

#funcion para conectarme  a la base de datos oracle
def get_connection(user_db,password_db,dsn_db):
    logging.info(f'Iniciando proceso de conexion a la base de datos {dsn_db}')
    try:
        conexion= oracledb.connect(
            user=user_db,
            password=password_db,
            dsn=dsn_db
        )
        logging.info(f'Conexion exitosa a la base de datos {dsn_db}')
        return conexion
    except Exception as ex:
        logging.error(ex)

#funcion para cerrar la conexion a base de datos oracle
def close_connection_db(conexion):
    logging.info('Iniciando proceso para cerrar conexion a base de datos')
    try:
        cierre_conexion= conexion.close()
        logging.info('Se cerro conexion de manera exitosa')
        return cierre_conexion
    except Exception as ex:
        logging.error(ex)

#funcion para leer archivo sql
def leer_sql(archivo_sql):
    logging.info('Se inicia la funcion de leer contenido de archivo sql')
    with open(archivo_sql, 'r',encoding='utf-8') as archivo:
        x=archivo.read()
        logging.info('Se ha leido todo el contenido del archivo sql')
        return x

#funcion para leer y ejecutar las consulta del archivo sql
def ejecutar_consultas(archivo_sql, conexion):
    logging.info('Se inicia la funcion de ejecutar consultas largas')
    try:
        
        with open(archivo_sql, 'r',encoding='utf-8') as archivo:
            consultas_sql = archivo.read().split(';')
        logging.info('Se abrio y se ha leido archivo sql')
        cursor = conexion.cursor()
        logging.info('Se crea cursor')
        for consulta in consultas_sql:
            if consulta.strip():  # Para evitar consultas vacías al final del archivo
                try:
                    cursor.execute(consulta)
                    logging.info(f'Se ejecuto correctamento la consulta')
                except oracledb.DatabaseError as error:
                    logging.info(f'Error al ejecutar el codigo {consulta}{error}')
        logging.info('Se ejecuto toda la consulta del archivo sql')
        conexion.commit()
        logging.info('Se confirma dichos cambios a la tabla')
        cursor.close()
        logging.info('Se cierra el cursor')
    except Exception as e:
        logging.error(e)
 


