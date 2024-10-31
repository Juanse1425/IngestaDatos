# utils.py
import os

def calcular_tamano_total_archivos(archivos):
    """Calcular el tamaño total de los archivos en MB"""
    tamano_total = sum(os.path.getsize(file) for file in archivos)
    return tamano_total / (1024 * 1024)  # Convertir a MB
