# validacion.py
import csv
import logging

def validar_csv(file_name, columnas_obligatorias):
    """Verificar que el CSV no tenga filas, columnas o celdas vacías, acumulando todos los errores"""
    errores = []
    with open(file_name, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        filas = list(reader)

        # Verificar que no haya filas vacías
        for i, fila in enumerate(filas):
            if not any(fila):  # Si toda la fila está vacía
                errores.append(f"Fila {i+1} está vacía.")

        # Verificar que no haya columnas vacías
        columnas = list(zip(*filas))
        for i, columna in enumerate(columnas):
            if not any(columna):  # Si toda la columna está vacía
                errores.append(f"Columna {i+1} está vacía.")

        # Verificar que no haya celdas vacías en columnas obligatorias
        headers = filas[0]
        indices_obligatorios = [headers.index(col) for col in columnas_obligatorias if col in headers]

        for i, fila in enumerate(filas[1:], start=2):  # Empieza en la fila 2 porque la primera es de encabezado
            for index in indices_obligatorios:
                if fila[index].strip() == "":
                    errores.append(f"Celda vacía en fila {i}, columna '{headers[index]}'.")

    if errores:
        logging.error("\n".join(errores))
        return False
    else:
        logging.info(f"Archivo CSV '{file_name}' validado correctamente.")
        return True
