import boto3
import os
from botocore.exceptions import NoCredentialsError

# Configuración segura de credenciales
s3_client = boto3.client(
        's3',
        aws_access_key_id='AKIAX3DNHBT2NAAYI5QC',  # Reemplaza con tu clave de acceso
        aws_secret_access_key='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',  # Reemplaza con tu clave secreta
        region_name='us-east-2'
    )

def upload_file_to_s3(file_name, bucket, object_name, metadata=None):
    """Subir un archivo a S3 con metadatos opcionales"""
    try:
        s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={'Metadata': metadata} if metadata else {})
        print(f"Archivo {file_name} subido a S3 en {bucket}/{object_name}")
        return True
    except FileNotFoundError:
        print(f"El archivo {file_name} no fue encontrado")
        return False
    except NoCredentialsError:
        print("Credenciales no disponibles")
        return False


def obtener_centro_investigacion(bucket, grupo_investigacion):
    try:
        # Listar objetos en el bucket filtrando el grupo de investigación
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=f'UQ/Raw/Academico/Investigacion/')

        # Buscar el centro de investigación en el nivel superior al grupo
        for obj in response.get('Contents', []):
            path_parts = obj['Key'].split('/')
            if len(path_parts) > 5 and path_parts[5] == f"Grupo_Investigacion={grupo_investigacion}":
                # Centro de investigación sería el nombre de la carpeta superior
                return path_parts[4].split('=')[1]
        print(f"No se encontró el centro de investigación para el grupo {grupo_investigacion}")
        return None
    except NoCredentialsError:
        print("Credenciales no disponibles")
        return None

def obtener_facultad(bucket, programa):
    try:
        # Listar objetos en el bucket filtrando el grupo de investigación
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=f'UQ/Raw/Academico/Facultades/')

        # Buscar el centro de investigación en el nivel superior al grupo
        for obj in response.get('Contents', []):
            path_parts = obj['Key'].split('/')
            if len(path_parts) > 5 and path_parts[5] == f"Programa={programa}":
                # Centro de investigación sería el nombre de la carpeta superior
                return path_parts[4].split('=')[1]
        print(f"No se encontró la facultad para el programa {programa}")
        return None
    except NoCredentialsError:
        print("Credenciales no disponibles")
        return None
