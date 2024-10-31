# log_handler.py
import logging
import os
import time
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

# Variable global para el archivo de log
log_file = ''

def configurar_logger():
    global log_file  # Declarar log_file como global
    current_directory = os.path.join(os.getcwd(), 'logs')  # Carpeta 'logs' en el directorio actual
    log_file = os.path.join(current_directory, 'log_generico.log')  # Nombre genérico

    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def generar_log(mensajes, nivel, bucket):
    """Generar un log con los mensajes dados"""
    configurar_logger()

    if nivel == 'info':
        logging.info(mensajes)
    elif nivel == 'error':
        logging.error(mensajes)

    upload_log_to_s3(bucket)

def contiene_errores_log(log_file):
    """Verificar si el archivo de log contiene errores"""
    with open(log_file, 'r') as file:
        logs = file.read()
        return "ERROR" in logs

def upload_log_to_s3(bucket):
    """Subir el archivo de log a S3"""
    s3_client = boto3.client(
        's3',
        aws_access_key_id='AKIAX3DNHBT2NAAYI5QC',  # Reemplaza con tu clave de acceso
        aws_secret_access_key='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',  # Reemplaza con tu clave secreta
        region_name='us-east-2'
    )

    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Comprobar si hay errores en el log antes de subirlo
    if contiene_errores_log(log_file):
        log_object_name = f'UQ/Logs/Errores/Log-{current_time}.log'  # Ruta para logs con errores
    else:
        log_object_name = f'UQ/Logs/Exitosos/Log-{current_time}.log'  # Ruta para logs exitosos

    try:
        s3_client.upload_file(log_file, bucket, log_object_name)
        print(f"Log {log_file} subido a S3 en {bucket}/{log_object_name}")
        eliminar_log_local()
    except FileNotFoundError:
        print(f"El archivo de log {log_file} no fue encontrado")
    except NoCredentialsError:
        print("Credenciales no disponibles")

def eliminar_log_local():
    """Eliminar el archivo de log local después de subirlo a S3"""
    global log_file  # Declarar log_file como global
    try:
        logging.shutdown()  # Cerrar el archivo de log antes de eliminarlo
        os.remove(log_file)
        print(f"Archivo de log {log_file} eliminado de forma local.")
    except OSError as e:
        print(f"Error al eliminar el archivo {log_file}: {e}")
