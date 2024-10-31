import csv
import os
import time
from datetime import datetime

import unicodedata

from LogHandler import generar_log
from S3Handler import upload_file_to_s3, obtener_facultad
from ValidarCSV import validar_csv

# Configuración
CARPETA_ARCHIVOS = './archivos/LandingZone/Programas'
BUCKET = 'datalakeuq'

# Columnas obligatorias
columnas_obligatorias = [
    'Nombre del estudiante', 'Código del estudiante', 'Materia', 'Nota',
    'Periodo académico', 'Programa académico'
]


def formatear_texto(texto):
    """Limpia el texto eliminando caracteres no ASCII, reemplazando espacios con guiones bajos y removiendo espacios en los extremos."""
    # Eliminar espacios al inicio y al final
    texto = texto.strip()
    # Reemplazar espacios con guiones bajos
    texto = texto.replace(" ", "_")
    # Convertir a ASCII eliminando caracteres especiales
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')
    return texto


def limpiar_ascii(texto):
    """Convierte texto a ASCII, eliminando caracteres no compatibles con S3."""
    return unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('ascii')


def obtener_metadatos(file_path, facultad, programa, periodo):
    """Obtiene los metadatos de creación del archivo y la fecha y hora de subida, en formato ASCII."""
    fecha_creacion = datetime.fromtimestamp(os.path.getctime(file_path)).strftime("%Y-%m-%d")
    hora_creacion = datetime.fromtimestamp(os.path.getctime(file_path)).strftime("%H:%M:%S")
    fecha_subida = datetime.now().strftime("%Y-%m-%d")
    hora_subida = datetime.now().strftime("%H:%M:%S")

    return {
        'FechaCreacion': fecha_creacion,
        'HoraCreacion': hora_creacion,
        'FechaSubida': fecha_subida,
        'HoraSubida': hora_subida,
        'Facultad': limpiar_ascii(facultad),
        'Programa': limpiar_ascii(programa),
        'Periodo': limpiar_ascii(periodo)
    }


def extraer_datos_csv(file_name):
    """Extrae el programa académico y el periodo académico de las primeras filas del CSV."""
    with open(file_name, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            programa = row.get('Programa académico')
            periodo = row.get('Periodo académico')
            if programa and periodo:
                return programa, periodo
    return None, None


def procesar_archivos(archivos, bucket):
    """Procesa y sube cada archivo a S3 si cumple con la validación CSV."""
    for file_name in archivos:
        file_basename = os.path.basename(file_name)

        if validar_csv(file_name, columnas_obligatorias):
            # Extraer programa y periodo del archivo CSV
            programa, periodo = extraer_datos_csv(file_name)
            if not programa or not periodo:
                generar_log(
                    f"No se pudo extraer 'Programa académico' o 'Periodo académico' del archivo '{file_basename}'.",
                    'error', bucket)
                continue
            programaFormateado = formatear_texto(programa)

            ano, semestre = periodo.split("-", 2)

            facultad = obtener_facultad(BUCKET, programaFormateado)
            # Definir el nombre del objeto y los metadatos
            object_name = f'UQ/Raw/Academico/Facultades/Facultad={facultad}/Programa={programaFormateado}/Year={ano}/Semestre={semestre}/{file_basename}'
            metadatos = obtener_metadatos(file_name, facultad, programaFormateado, periodo)

            # Subir a S3
            if upload_file_to_s3(file_name, bucket, object_name, metadatos):
                generar_log(f"Archivo {file_basename} subido exitosamente.", 'info', bucket)
                os.remove(file_name)  # Eliminar archivo tras subida exitosa
            else:
                generar_log(f"Fallo al subir archivo {file_basename}.", 'error', bucket)
        else:
            generar_log(f"El archivo CSV '{file_basename}' no es válido.", 'error', bucket)


def iniciar_orquestador():
    """Inicia el orquestador que monitorea la carpeta y procesa los archivos en lotes."""
    while True:
        archivos = [os.path.join(CARPETA_ARCHIVOS, file) for file in os.listdir(CARPETA_ARCHIVOS) if
                    os.path.isfile(os.path.join(CARPETA_ARCHIVOS, file))]

        if len(archivos) > 0:
            procesar_archivos(archivos, BUCKET)
        else:
            print("No hay ningun archivo que subir")

        # Esperar un tiempo antes de volver a revisar (ej. 10 segundos)
        time.sleep(10)


if __name__ == "__main__":
    iniciar_orquestador()
