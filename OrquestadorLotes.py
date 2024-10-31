import os
import time
from datetime import datetime

from LogHandler import generar_log
from S3Handler import upload_file_to_s3, obtener_centro_investigacion
from Utils import calcular_tamano_total_archivos

# Configuración
CARPETA_ARCHIVOS = './archivos/LandingZone/Investigaciones'
LIMITE_ARCHIVOS = 5
LIMITE_TAMANO_MB = 100
BUCKET = 'datalakeuq'
lote_numero = 1


def obtener_metadatos(file_path, lote, centro_investigacion, grupo_investigacion, nombre_proyecto):
    """Obtiene los metadatos de creación del archivo y la fecha y hora de subida."""
    # Fecha y hora de creación del archivo
    fecha_creacion = datetime.fromtimestamp(os.path.getctime(file_path)).strftime("%Y-%m-%d")
    hora_creacion = datetime.fromtimestamp(os.path.getctime(file_path)).strftime("%H:%M:%S")

    return {
        'FechaCreacion': fecha_creacion,
        'HoraCreacion': hora_creacion,
        'Lote': lote,
        'CentroInvestigacion': centro_investigacion,
        'GrupoInvestigacion': grupo_investigacion,
        'NombreProyecto': nombre_proyecto

    }


def procesar_lote(archivos, bucket):
    """Sube un lote de archivos a S3 y los elimina de la carpeta de origen."""
    global lote_numero
    archivos = archivos[:LIMITE_ARCHIVOS]

    # Fecha y hora de subida
    fecha_subida = datetime.now().strftime("%Y-%m-%d")
    hora_subida = datetime.now().strftime("%H:%M:%S")
    lote = f"{fecha_subida} {hora_subida}"

    for file_name in archivos:
        file_basename = os.path.basename(file_name)

        # Extraer grupo de investigación y nombre del proyecto sin la extensión
        try:
            grupo_investigacion, nombre_proyecto, nombre_archivo = file_basename.split("-", 2)
        except ValueError:
            generar_log(f"Formato de nombre de archivo incorrecto: {file_basename}", 'error', BUCKET)
            continue

        # Obtener el centro de investigación desde S3
        centro_investigacion = obtener_centro_investigacion(bucket, grupo_investigacion)
        if not centro_investigacion:
            generar_log(f"No se encontró centro de investigación para el grupo {grupo_investigacion}", 'error', BUCKET)
            continue

        # Crear metadatos
        metadatos = obtener_metadatos(file_name, lote, centro_investigacion, grupo_investigacion, nombre_proyecto)
        object_name = f'UQ/Raw/Academico/Investigacion/Centro_Investigacion={centro_investigacion}/Grupo_Investigacion={grupo_investigacion}/{grupo_investigacion}={nombre_proyecto}/{nombre_archivo}'

        # Subir archivo a S3 con metadatos y eliminarlo si tiene éxito
        if upload_file_to_s3(file_name, bucket, object_name, metadatos):
            generar_log(f"Archivo {file_name} subido exitosamente.", 'info', BUCKET)
            os.remove(file_name)
        else:
            generar_log(f"Fallo al subir archivo {file_name}.", 'error', BUCKET)

    lote_numero += 1


def iniciar_orquestador():
    """Inicia el orquestador que monitorea la carpeta y procesa los archivos en lotes."""
    while True:
        archivos = [os.path.join(CARPETA_ARCHIVOS, file) for file in os.listdir(CARPETA_ARCHIVOS) if
                    os.path.isfile(os.path.join(CARPETA_ARCHIVOS, file))]

        # Verificar si hay al menos LIMITE_ARCHIVOS o si el tamaño del lote supera el límite
        if len(archivos) >= LIMITE_ARCHIVOS or calcular_tamano_total_archivos(archivos) > LIMITE_TAMANO_MB:
            print("Procesando lote de archivos...")
            procesar_lote(archivos, BUCKET)
        else:
            print(
                f"Aún no se ha alcanzado el límite. Archivos actuales: {len(archivos)}, Tamaño actual: {calcular_tamano_total_archivos(archivos)} MB")

        # Esperar un tiempo antes de volver a revisar (ej. 10 segundos)
        time.sleep(10)


if __name__ == "__main__":
    iniciar_orquestador()
