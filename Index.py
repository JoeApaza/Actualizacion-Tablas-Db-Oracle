import logging
import os
from dotenv import load_dotenv
from src.database.db_oracle import close_connection_db,get_connection,ejecutar_consultas
from src.routes.Rutas import ruta_Recaudacion_Corporativa,ruta_Actualizacion_Cuentas
from datetime import datetime, timedelta
 
# Obtén la fecha actual
fecha_actual = datetime.now()
fecha_ayer = fecha_actual - timedelta(days=1)
año = fecha_ayer.strftime('%Y')
mes = fecha_ayer.strftime('%m')
dia = fecha_ayer.strftime('%d')


logging.basicConfig(format="%(asctime)s::%(levelname)s::%(message)s",   
                    datefmt="%d-%m-%Y %H:%M:%S",    
                    level=10,   
                    filename='./src/utils/log/app.log',filemode='w')


load_dotenv()
Conexion_Opercom=get_connection(os.getenv('USER'),os.getenv('PASSWORD'),os.getenv('DNS'))

ejecutar_consultas(ruta_Actualizacion_Cuentas, Conexion_Opercom)
ejecutar_consultas(ruta_Recaudacion_Corporativa, Conexion_Opercom)
close_connection_db(Conexion_Opercom)
