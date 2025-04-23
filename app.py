import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import pytz
import re
from config import cargar_configuracion
import traceback

# Cargar configuración
aws_access_key, aws_secret_key, region_name, bucket_name, valid_user, valid_password = cargar_configuracion()

# Configuración de AWS S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

# Función para cargar un archivo en S3
def upload_file_to_s3(file, filename, original_filename):
    try:
        s3.upload_fileobj(file, bucket_name, filename)
        st.success(f"Archivo '{original_filename}' subido exitosamente.")
    except Exception as e:
        st.error(f"Error al subir el archivo: {e}")

# Función para cargar un archivo en S3
def upload_file_to_s3_RRHH(file, filename, original_filename):
    try:
        s3.upload_fileobj(file, bucket_name, filename)
        st.success(f"Tabla de RRHH, del archivo '{original_filename}' subido exitosamente.")
    except Exception as e:
        st.error(f"Error al subir el archivo: {e}")

# Función para cargar un archivo en S3
def upload_file_to_s3_Aceleradores(file, filename, original_filename):
    try:
        s3.upload_fileobj(file, bucket_name, filename)
        st.success(f"Tabla de Aceleradores, del archivo '{original_filename}' subido exitosamente.")
    except Exception as e:
        st.error(f"Error al subir el archivo: {e}")
        
# Función para guardar errores en un archivo log en S3
def log_error_to_s3(error_message, filename):
    try:
        log_filename = "Errores.txt"
        now = datetime.now()
        log_entry = pd.DataFrame([{
            "Fecha": now.strftime('%Y-%m-%d'),
            "Hora": now.strftime('%H:%M'),
            "Error": error_message,
            "NombreArchivo": filename
        }])
        
        # Descargar el archivo log existente si existe
        try:
            log_obj = s3.get_object(Bucket=bucket_name, Key=log_filename)
            log_df = pd.read_csv(BytesIO(log_obj['Body'].read()))
            log_df = pd.concat([log_df, log_entry], ignore_index=True)
        except s3.exceptions.NoSuchKey:
            log_df = log_entry  # Crear un nuevo DataFrame si no existe el archivo

        # Subir el archivo log actualizado
        csv_buffer = BytesIO()
        log_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        csv_buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=log_filename, Body=csv_buffer.getvalue())
    except Exception as e:
        st.error(f"Error al guardar el log en S3: {e}")

# Verificar formato del nombre del archivo
def validate_filename(filename):
    pattern = r"^\d{2}-\d{2}-\d{4}\+.+\+.+\.xlsx$"
    return re.match(pattern, filename)

# Función para validar la fecha del archivo
def validate_file_date(filename):
    try:
        file_date_str = filename.split('+')[0]
        file_date = datetime.strptime(file_date_str, '%d-%m-%Y')
        now = datetime.now()
        current_month = now.month
        current_year = now.year

        # Check if the file date is from the current month, more than two months prior, or a future month
        if (file_date.year == current_year and file_date.month == current_month) or \
           (file_date.year == current_year and file_date.month > current_month) or \
           (file_date.year > current_year) or \
           (file_date.year == current_year and file_date.month < current_month - 1) or \
           (file_date.year == current_year - 1 and current_month in [1, 2] and file_date.month < 12 - (1 - current_month)):
            return False

        return True
    except Exception as e:
        st.error(f"Error al validar la fecha del archivo: {e}")
        return False

# Extraer el nombre del líder del archivo
def extract_leader_name(filename):
    try:
        return filename.split('+')[-1].replace('.xlsx', '')
    except IndexError:
        return None

# Extraer la fecha y la sucursal del archivo
def extract_date_and_sucursal(filename):
    try:
        parts = filename.split('+')
        fecha = parts[0]
        sucursal = parts[1]
        return fecha, sucursal
    except IndexError:
        return None, None

# Verificar celdas del formulario
def validate_form_cells(sheet_data, sheet_name, filename):
    try:
        required_cells = ['B1', 'B2', 'B3', 'B4']
        for cell in required_cells:
            if pd.isna(sheet_data.at[int(cell[1])-1, 1]):
                error_message = f"Error: La celda {cell} en la hoja '{sheet_name}' está vacía."
                st.error(error_message)
                log_error_to_s3(error_message, filename)
                return False

        cuil = str(sheet_data.at[1, 1])
        if not re.match(r"^\d{11}$", cuil):
            error_message = f"Error: La celda B2 en la hoja '{sheet_name}' debe contener 11 números."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        # Validar que los campos de comisiones y horas extra sean números o nulos
        comisiones_accesorias = sheet_data.at[0, 10]
        hs_extras_50 = sheet_data.at[1, 10]
        hs_extras_100 = sheet_data.at[2, 10]
        incentivo_productividad = sheet_data.at[3, 10]
        ajuste_incentivo = sheet_data.at[4, 10]

        if not (pd.isna(comisiones_accesorias) or (isinstance(comisiones_accesorias, (int, float)) and float(comisiones_accesorias).is_integer())):
            error_message = f"Error: La celda K1 en la hoja '{sheet_name}' debe contener un número entero."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        if not (pd.isna(hs_extras_50) or isinstance(hs_extras_50, (int, float))):
            error_message = f"Error: La celda K2 en la hoja '{sheet_name}' debe contener solo números."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        if not (pd.isna(hs_extras_100) or isinstance(hs_extras_100, (int, float))):
            error_message = f"Error: La celda K3 en la hoja '{sheet_name}' debe contener solo números."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        if not (pd.isna(incentivo_productividad) or (isinstance(incentivo_productividad, (int, float)) and float(incentivo_productividad).is_integer())):
            error_message = f"Error: La celda K4 en la hoja '{sheet_name}' debe contener un número entero."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        if not (pd.isna(ajuste_incentivo) or (isinstance(ajuste_incentivo, (int, float)) and float(ajuste_incentivo).is_integer())):
            error_message = f"Error: La celda K5 en la hoja '{sheet_name}' debe contener un número entero."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        return True
    except Exception as e:
        error_message = f"Error al validar las celdas del formulario en la hoja '{sheet_name}': {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False

# Verificar columnas requeridas
def validate_required_columns(data):
    required_columns = [
        'Tipo Indicador', 'Tipo Dato', 'Indicadores de Gestion', 'Ponderacion',
        'Objetivo Aceptable (70%)', 'Objetivo Muy Bueno (90%)', 'Objetivo Excelente (120%)',
        'Resultado', '% Logro', 'Calificación', 'Ultima Fecha de Actualización',
        'Lider Revisor', 'Comentario'
    ]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        return False, missing_columns
    return True, []

# Función para verificar si hay ponderaciones con 0%
def validate_ponderacion(data, filename):
    if (data['Ponderacion'] == 0).any():
        error_message = "Error: Existen filas con Ponderacion 0%."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False
    return True

# Función para verificar si la suma de la columna Ponderacion es 1
def validate_ponderacion_sum(data, filename, sheet_name):
    ponderacion_sum = data['Ponderacion'].sum()
    if not (0.99 <= ponderacion_sum <= 1.1):
        error_message = f"Error: La suma de la columna Ponderacion en la hoja '{sheet_name}' es {ponderacion_sum * 100:.2f}%, no es 100%."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False
    return True

# Función para verificar estructura interna de cada hoja
def verify_sheet_structure(sheet_data, sheet_name, filename):
    if sheet_data.empty or sheet_data.shape[1] < 1:
        error_message = f"Error: La hoja '{sheet_name}' está vacía o no tiene suficientes columnas."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False
    return True

# Función para extraer datos del formulario
def extract_data_from_form(sheet_data):
    try:
        cargo = sheet_data.iloc[0, 1]
        cuil = sheet_data.iloc[1, 1]
        segmento = sheet_data.iloc[2, 1]
        area_influencia = sheet_data.iloc[3, 1]
        comisiones_accesorias = sheet_data.iloc[0, 10]
        hs_extras_50 = sheet_data.iloc[1, 10]
        hs_extras_100 = sheet_data.iloc[2, 10]
        incentivo_productividad = sheet_data.iloc[3, 10]
        ajuste_incentivo = sheet_data.iloc[4, 10]
        return cargo, cuil, segmento, area_influencia, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo
    except IndexError:
        return None, None, None, None, None, None, None, None, None

# Función para contar filas hasta encontrar una vacía
def count_rows_until_empty(data, column_name="Indicadores de Gestion"):
    try:
        header_index = data[data.iloc[:, 2] == column_name].index[0]
        relevant_rows = data.iloc[header_index + 1:, 2]
        return relevant_rows.isna().idxmax() - (header_index + 1)
    except Exception as e:
        st.error(f"Error contando filas hasta vacío: {e}")
        return 0

# Función para limpiar y reestructurar datos
def clean_and_restructure_until_empty(data, cargo, cuil, segmento, area_influencia, leader_name, fecha, sucursal, filename, upload_datetime, sheet_name, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo):
    try:
        header_row = data[data.iloc[:, 0] == 'Tipo Indicador'].index[0]
        rows_to_process = count_rows_until_empty(data, "Indicadores de Gestion")

        if rows_to_process == 0:
            error_message = "Error: No se encontraron filas válidas después del encabezado."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return pd.DataFrame()

        data.columns = data.iloc[header_row]
        data = data.iloc[header_row + 1:header_row + 1 + rows_to_process].reset_index(drop=True)

        column_mapping = {
            'Tipo Indicador': 'Tipo Indicador',
            'Tipo Dato': 'Tipo Dato',
            'Indicadores de Gestion': 'Indicadores de Gestion',
            'Ponderacion': 'Ponderacion',
            'Objetivo Aceptable (70%)': 'Objetivo Aceptable (70%)',
            'Objetivo Muy Bueno (90%)': 'Objetivo Muy Bueno (90%)',
            'Objetivo Excelente (120%)': 'Objetivo Excelente (120%)',
            'Resultado': 'Resultado',
            '% Logro': '% Logro',
            'Calificación': 'Calificación',
            'Ultima Fecha de Actualización': 'Ultima Fecha de Actualización',
            'Lider Revisor': 'Lider Revisor',
            'Comentario': 'Comentario'
        }
        data = data.rename(columns=column_mapping)

        valid_columns, missing_columns = validate_required_columns(data)
        if not valid_columns:
            error_message = f"Error: Faltan las siguientes columnas requeridas: {', '.join(missing_columns)}"
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return pd.DataFrame()

        if not validate_ponderacion(data, filename):
            return pd.DataFrame()

        if not validate_ponderacion_sum(data, filename, sheet_name):
            return pd.DataFrame()

        data['Cargo'] = cargo
        data['CUIL'] = cuil
        data['Segmento'] = segmento
        data['Área de influencia'] = area_influencia
        data['Nombre Lider'] = leader_name
        data['Fecha_Nombre_Archivo'] = fecha
        data['Sucursal'] = sucursal
        data['Fecha Horario Subida'] = upload_datetime
        data['COMISIONES ACCESORIAS'] = comisiones_accesorias
        data['HS EXTRAS AL 50'] = hs_extras_50
        data['HS EXTRAS AL 100'] = hs_extras_100
        data['INCENTIVO PRODUCTIVIDAD'] = incentivo_productividad
        data['AJUSTE INCENTIVO'] = ajuste_incentivo

        desired_columns = [
            'Cargo', 'CUIL', 'Segmento', 'Área de influencia', 'Nombre Lider', 'Fecha_Nombre_Archivo', 'Sucursal',
            'Fecha Horario Subida', 'Tipo Indicador', 'Tipo Dato', 'Indicadores de Gestion', 'Ponderacion',
            'Objetivo Aceptable (70%)', 'Objetivo Muy Bueno (90%)', 'Objetivo Excelente (120%)',
            'Resultado', '% Logro', 'Calificación',
            'Ultima Fecha de Actualización', 'Lider Revisor', 'Comentario',
            'COMISIONES ACCESORIAS', 'HS EXTRAS AL 50', 'HS EXTRAS AL 100', 'INCENTIVO PRODUCTIVIDAD', 'AJUSTE INCENTIVO'
        ]
        return data[desired_columns]
    except Exception as e:
        error_message = f"Error al limpiar y reestructurar: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return pd.DataFrame()

# Función para verificar si hay CUILs repetidos en diferentes hojas
def validate_unique_cuils(dataframes):
    cuils = []
    for df in dataframes:
        cuils.extend(df['CUIL'].unique())
    if len(cuils) != len(set(cuils)):
        return False
    return True

# Función para procesar hojas del Excel
def process_sheets_until_empty(excel_data, filename, upload_datetime):
    final_data = pd.DataFrame()
    leader_name = extract_leader_name(filename)
    fecha, sucursal = extract_date_and_sucursal(filename)
    dataframes = []
    for sheet_name in excel_data.sheet_names:
        sheet_data = excel_data.parse(sheet_name, header=None)
        if not verify_sheet_structure(sheet_data, sheet_name, filename):
            return pd.DataFrame(), False  # Return empty DataFrame and error state
        if not validate_form_cells(sheet_data, sheet_name, filename):
            return pd.DataFrame(), False  # Return empty DataFrame and error state
        cargo, cuil, segmento, area_influencia, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo = extract_data_from_form(sheet_data)
        if cargo and cuil and segmento and area_influencia:
            processed_data = clean_and_restructure_until_empty(sheet_data, cargo, cuil, segmento, area_influencia, leader_name, fecha, sucursal, filename, upload_datetime, sheet_name, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo)
            if processed_data.empty:
                return pd.DataFrame(), False  # Return empty DataFrame and error state
            if not validate_update_dates(processed_data, filename, sheet_name):
                return pd.DataFrame(), False  # Return empty DataFrame and error state
            dataframes.append(processed_data)
            final_data = pd.concat([final_data, processed_data], ignore_index=True)
    
    if not validate_unique_cuils(dataframes):
        error_message = "Error: Existen CUILs repetidos en diferentes hojas del archivo."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return pd.DataFrame(), False  # Return empty DataFrame and error state

    return final_data, True  # Return DataFrame and success state

# Función para determinar si el tablero es "Ajuste" o "Normal"
def determine_tablero_type(fecha, upload_datetime):
    fecha_tablero = datetime.strptime(fecha, '%d-%m-%Y')
    ajuste_fecha = datetime.strptime('22/04/2025', '%d/%m/%Y')  # Ingresar la fecha de ajuste aquí
    if upload_datetime > ajuste_fecha:
        return "Ajuste"
    return "Normal"

# Función para verificar fechas en la columna "Ultima Fecha de Actualización"
def validate_update_dates(data, filename, sheet_name):
    try:
        argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        now = datetime.now(argentina_tz)
        now = pd.to_datetime(now.strftime('%Y-%m-%d'))  # Convertir la fecha actual al mismo tipo de datos

        # Verificar si la columna existe
        if 'Ultima Fecha de Actualización' not in data.columns:
            error_message = f"Error: La columna 'Ultima Fecha de Actualización' no existe en la hoja '{sheet_name}'."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        # Verificar valores nulos
        if data['Ultima Fecha de Actualización'].isna().any():
            error_message = f"Error: Existen valores nulos en la columna 'Ultima Fecha de Actualización' en la hoja '{sheet_name}'."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        # Verificar formato de fecha
        data['Ultima Fecha de Actualización'] = pd.to_datetime(
            data['Ultima Fecha de Actualización'], format='%d/%m/%Y', errors='coerce'
        )
        if data['Ultima Fecha de Actualización'].isna().any():
            error_message = f"Error: Existen valores en la columna 'Ultima Fecha de Actualización' en la hoja '{sheet_name}' que no tienen el formato de fecha válido (%d/%m/%Y)."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        # Verificar fechas futuras
        invalid_dates = data[data['Ultima Fecha de Actualización'] > now]
        if not invalid_dates.empty:
            error_message = f"Error: Existen fechas en la columna 'Ultima Fecha de Actualización' en la hoja '{sheet_name}' que son posteriores a la fecha actual."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        return True
    except Exception as e:
        error_message = f"Error al validar las fechas en la columna 'Ultima Fecha de Actualización' en la hoja '{sheet_name}': {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False

# Función para verificar duplicados en S3
def check_for_duplicates(cuil, fecha, leader_name):
    try:
        cuil = str(cuil)
        fecha = str(fecha)
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                obj_key = obj['Key']
                obj_data = s3.get_object(Bucket=bucket_name, Key=obj_key)
                df = pd.read_csv(BytesIO(obj_data['Body'].read()))
                if 'CUIL' in df.columns and 'Fecha_Nombre_Archivo' in df.columns:
                    df['CUIL'] = df['CUIL'].astype(str)
                    df['Fecha_Nombre_Archivo'] = df['Fecha_Nombre_Archivo'].astype(str)
                    if not df.empty and df['CUIL'].str.contains(cuil).any() and df['Fecha_Nombre_Archivo'].str.contains(fecha).any():
                        existing_leader = df.loc[df['CUIL'] == cuil, 'Nombre Lider'].values[0]
                        if existing_leader != leader_name:
                            return True, existing_leader, cuil  # Block upload if the leader is different
        return False, None, None
    except Exception as e:
        st.error(f"Error al verificar duplicados en S3: {e}")
        return False, None, None

# Función para verificar si el archivo es de vendedores
def is_vendedores_file(filename):
    return "Vendedores" in filename

# Modificar la función validate_resumen_rrhh
def validate_resumen_rrhh(sheet_data, filename):
    required_columns = [
        "Sucursal", "Vendedores", "CUIT", "LEGAJO", "Total Ventas", "Vta PPAA", "Descuentos PPAA",
        "COMISION PPAA", "0km", "Usados", "Premio Convencional", "Comision Convencional", "Total a liquidar"
    ]
    try:
        # Buscar la fila que contiene los encabezados
        header_row_index = None
        for i in range(len(sheet_data)):
            row = sheet_data.iloc[i, :13].tolist()  # Tomar las primeras 13 columnas
            if all(isinstance(cell, str) for cell in row) and all(cell.strip() != "" for cell in row):
                header_row_index = i
                break

        if header_row_index is None:
            st.error("Error: No se encontraron encabezados válidos en la hoja 'Resumen RRHH'.")
            return None  # Retornar None si no se encuentran encabezados

        # Leer los encabezados y limpiar espacios
        headers = sheet_data.iloc[header_row_index, :13].apply(lambda x: str(x).strip()).tolist()

        # Verificar si los encabezados coinciden con los requeridos
        if headers != required_columns:
            missing_columns = [col for col in required_columns if col not in headers]
            error_message = f"Error: Faltan las siguientes columnas en 'Resumen RRHH': {', '.join(missing_columns)}"
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return None  # Retornar None si faltan columnas

        # Tomar los datos debajo de los encabezados
        data = sheet_data.iloc[header_row_index + 1:, :13]
        data.columns = headers  # Asignar los encabezados a las columnas

        return data  # Retornar el DataFrame procesado
    except Exception as e:
        # Agregar más detalles al mensaje de error
        error_message = f"Error al validar 'Resumen RRHH': {e}. Traceback: {traceback.format_exc()}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return None  # Retornar None en caso de error

# Validar hojas de colaboradores
def validate_colaborador_sheet(sheet_data, sheet_name, filename, empresa, tipo_tablero):
    try:
        # Detectar la fila que contiene los encabezados
        header_row_index = None
        for i in range(len(sheet_data)):
            row = sheet_data.iloc[i, :].tolist()
            if "Indicadores de Gestion" in row:
                header_row_index = i
                break

        if header_row_index is None:
            st.error(f"Error: No se encontraron encabezados válidos en la hoja '{sheet_name}'.")
            return False

        # Validar que el CUIL en la celda B1 sea un número de 11 dígitos
        cuil = str(sheet_data.iloc[0, 1]).strip()  # Celda B1 corresponde a la fila 0, columna 1
        cuil = re.sub(r"[^\d]", "", cuil)  # Eliminar cualquier carácter no numérico
        if not re.fullmatch(r"^\d{11}$", cuil):  # Verificar que sea un número de 11 dígitos
            error_message = f"Error: El CUIL en la celda B1 de la hoja '{sheet_name}' debe ser un número de 11 dígitos sin guiones, puntos ni letras. Valor encontrado: {cuil}"
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        # Configurar los encabezados y limpiar espacios
        sheet_data.columns = sheet_data.iloc[header_row_index].apply(lambda x: str(x).strip())
        sheet_data = sheet_data.iloc[header_row_index + 1:].reset_index(drop=True)

        # Verificar si la columna "Indicadores de Gestion" existe
        if "Indicadores de Gestion" not in sheet_data.columns:
            st.error(f"Error: La columna 'Indicadores de Gestion' no existe en la hoja '{sheet_name}'.")
            return False

        # Validar indicadores obligatorios
        required_indicators = ["Q Ventas 0 km", "Q Ventas Usados", "Q Suscripciones"]
        if empresa == "Autosol" and tipo_tablero == "DIRECTA":
            required_indicators.append("Tomas Usados")
        elif empresa == "Autociel":
            required_indicators.extend([
                "Mora de Cartera (6 meses móviles)", "NPS", "Días hábiles de cancelación",
                "Créditos PSA", "Créditos KM"
            ])
        if not all(indicator in sheet_data["Indicadores de Gestion"].values for indicator in required_indicators):
            missing_indicators = [ind for ind in required_indicators if ind not in sheet_data["Indicadores de Gestion"].values]
            st.error(f"Error en la hoja '{sheet_name}': Faltan los siguientes indicadores: {', '.join(missing_indicators)}")
            return False

        # Validar formulario específico
        if not validate_formulario(sheet_data, empresa, tipo_tablero, filename):
            return False

        return True
    except Exception as e:
        error_message = f"Error al validar la hoja '{sheet_name}': {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False

# Validar formulario específico por empresa y tipo
def validate_formulario(sheet_data, empresa, tipo, filename):
    try:
        if empresa == "Autolux":
            if tipo == "PPAA":
                expected_headers = ["NPS", "Volumen", "70% adh", "Débito", "Mix Tablero", "T. Acelerador"]
                header_range = (7, 12)  # H1-M1
            elif tipo == "DIRECTA":
                expected_headers = ["SSI", "Rentabilidad", "Tablero", "TOT", "Acelerador"]
                header_range = (7, 10)  # H1-K1
        elif empresa == "Autosol":
            if tipo == "PPAA":
                expected_headers = ["CEM", "70% adh", "Débito", "Tablero", "T. Acelerador"]
                header_range = (7, 11)  # H1-K1
            elif tipo == "DIRECTA":
                expected_headers = ["CEM", "Rent. Global", "Prom. 0KM", "Tablero", "T. Acelerador"]
                header_range = (7, 11)  # H1-K1
        elif empresa == "Autociel":
            expected_headers = ["Q Tomas", "70% adh", "Débito", "Premio Volumen", "T. Acelerador"]
            header_range = (7, 11)  # H1-K1
        else:
            st.error(f"Empresa desconocida: {empresa}")
            return False

        # Validar encabezados
        headers = sheet_data.iloc[0, header_range[0]:header_range[1] + 1].values
        if not all(expected == actual for expected, actual in zip(expected_headers, headers)):
            st.error(f"Error en el formulario: Los encabezados no coinciden con los esperados para {empresa} ({tipo}).")
            return False

        # Validar valores
        values = sheet_data.iloc[1, header_range[0]:header_range[1] + 1].values
        if not all(isinstance(value, (int, float)) for value in values):
            st.error(f"Error en el formulario: Los valores deben ser numéricos.")
            return False

        return True
    except Exception as e:
        st.error(f"Error al validar el formulario: {e}")
        return False

# Extraer el nombre de la empresa del archivo
def extract_empresa_from_filename(filename):
    try:
        # El nombre de la empresa está después de "Vendedores " y antes del siguiente "+"
        parts = filename.split('+')
        if len(parts) > 1 and "Vendedores" in parts[1]:
            empresa = parts[1].split(' ')[1]  # Extraer la palabra después de "Vendedores"
            return empresa
        else:
            return None
    except Exception as e:
        st.error(f"Error al extraer la empresa del nombre del archivo: {e}")
        return None

def generate_aceleradores_csv(aceleradores_data, original_filename):
    """
    Genera un único archivo CSV con los valores de todas las hojas del archivo Excel para tableros de vendedores.
    """
    try:
        # Verificar que el DataFrame no esté vacío
        if aceleradores_data.empty:
            st.error("Error: No se encontraron datos para generar el archivo CSV de aceleradores.")
            return False

        # Generar el nombre del archivo CSV
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        csv_filename_aceleradores = f"Aceleradores-{timestamp}_{original_filename.split('.')[0]}.csv"

        # Guardar el CSV en un buffer
        csv_buffer_aceleradores = BytesIO()
        aceleradores_data.to_csv(csv_buffer_aceleradores, index=False, encoding="utf-8-sig")
        csv_buffer_aceleradores.seek(0)

        # Subir el archivo CSV a S3
        upload_file_to_s3_Aceleradores(csv_buffer_aceleradores, csv_filename_aceleradores, original_filename)
        return True

    except Exception as e:
        error_message = f"Error al generar el archivo CSV de aceleradores: {e}. Traceback: {traceback.format_exc()}"
        st.error(error_message)
        log_error_to_s3(error_message, original_filename)
        return False

# Modificar la función process_and_upload_excel para incluir la nueva funcionalidad
def process_and_upload_excel(file, original_filename):
    try:
        # Leer el archivo Excel
        excel_data = pd.ExcelFile(file)

        # Procesar la hoja "Resumen RRHH" si existe
        if "Resumen RRHH" in excel_data.sheet_names:
            resumen_rrhh_data = excel_data.parse("Resumen RRHH", header=None, decimal=",")  # Leer con coma como separador decimal

            # Validar la estructura de la hoja "Resumen RRHH"
            resumen_rrhh_data = validate_resumen_rrhh(resumen_rrhh_data, original_filename)
            if resumen_rrhh_data is None:
                st.error("Error en la hoja 'Resumen RRHH'. No se procesará el archivo.")
                return  # Detener la ejecución si hay un error

            # Guardar la hoja "Resumen RRHH" como un archivo CSV separado
            csv_buffer_rrhh = BytesIO()
            resumen_rrhh_data.to_csv(csv_buffer_rrhh, index=False, encoding="utf-8-sig")  # Guardar sin índices
            csv_filename_rrhh = f"RRHH-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename.split('.')[0]}.csv"
            csv_buffer_rrhh.seek(0)
            upload_file_to_s3_RRHH(csv_buffer_rrhh, csv_filename_rrhh, original_filename)

        # Procesar las demás hojas
        final_data = pd.DataFrame()
        aceleradores_data = pd.DataFrame()  # DataFrame para almacenar los datos de aceleradores

        for sheet_name in excel_data.sheet_names:
            if sheet_name == "Resumen RRHH":
                continue

            # Leer los datos de la hoja
            sheet_data = excel_data.parse(sheet_name, header=None)

            # Validar estructura de la hoja
            if not verify_sheet_structure(sheet_data, sheet_name, original_filename):
                st.error(f"Error en la estructura de la hoja '{sheet_name}'. No se procesará el archivo.")
                return  # Detener la ejecución si hay un error

            # Verificar si el archivo es de vendedores
            if is_vendedores_file(original_filename):
                # Extraer el CUIL, Tipo Vendedor y los indicadores de la hoja
                try:
                    cuil = str(sheet_data.iloc[0, 1]).strip()  # Celda B1 corresponde a la fila 0, columna 1
                    cuil = re.sub(r"[^\d]", "", cuil)  # Eliminar cualquier carácter no numérico
                    tipo_vendedor = str(sheet_data.iloc[1, 1]).strip()  # Celda B2 corresponde a la fila 1, columna 1
                    indicadores = sheet_data.iloc[1, 7:13].tolist()  # H2 a M2 corresponden a las columnas 7 a 12 (índice 0)

                    if not re.fullmatch(r"^\d{11}$", cuil):  # Verificar que sea un número de 11 dígitos
                        error_message = f"Error: El CUIL en la celda B1 de la hoja '{sheet_name}' debe ser un número de 11 dígitos sin guiones, puntos ni letras. Valor encontrado: {cuil}"
                        st.error(error_message)
                        log_error_to_s3(error_message, original_filename)
                        continue

                    if not tipo_vendedor:
                        error_message = f"Error: El Tipo Vendedor en la celda B2 de la hoja '{sheet_name}' está vacío o no es válido."
                        st.error(error_message)
                        log_error_to_s3(error_message, original_filename)
                        continue

                    if len(indicadores) != 6:
                        st.error(f"Error: No se encontraron exactamente 6 indicadores en las celdas H2 a M2 de la hoja '{sheet_name}'.")
                        continue

                    if not all(isinstance(ind, (int, float)) or pd.isna(ind) for ind in indicadores):
                        error_message = f"Error: Los indicadores en las celdas H2 a M2 de la hoja '{sheet_name}' deben ser numéricos o nulos."
                        st.error(error_message)
                        log_error_to_s3(error_message, original_filename)
                        continue

                    # Agregar los datos al DataFrame de aceleradores
                    aceleradores_data = pd.concat([
                        aceleradores_data,
                        pd.DataFrame([{
                            "CUIL": cuil,
                            "Tipo Vendedor": tipo_vendedor,
                            "Indicador 1": indicadores[0],
                            "Indicador 2": indicadores[1],
                            "Indicador 3": indicadores[2],
                            "Indicador 4": indicadores[3],
                            "Indicador 5": indicadores[4],
                            "Indicador 6": indicadores[5]
                        }])
                    ], ignore_index=True)

                except Exception as e:
                    error_message = f"Error al procesar la hoja '{sheet_name}': {e}"
                    st.error(error_message)
                    log_error_to_s3(error_message, original_filename)
                    continue

            # Extraer datos del formulario
            cargo, cuil, segmento, area_influencia, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo = extract_data_from_form(sheet_data)

            # Limpiar y reestructurar los datos del tablero
            processed_data = clean_and_restructure_until_empty(
                sheet_data, cargo, cuil, segmento, area_influencia,
                extract_leader_name(original_filename),  # Nombre del líder
                *extract_date_and_sucursal(original_filename),  # Fecha y sucursal
                original_filename, datetime.now().strftime('%d/%m/%Y_%H:%M:%S'), sheet_name,
                comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo
            )

            if processed_data.empty:
                st.error(f"Error: No se pudieron procesar los datos del tablero en la hoja '{sheet_name}'. No se procesará el archivo.")
                return  # Detener la ejecución si hay un error

            # Concatenar los datos procesados
            final_data = pd.concat([final_data, processed_data], ignore_index=True)

        # Generar el archivo CSV de aceleradores
        if not aceleradores_data.empty:
            generate_aceleradores_csv(aceleradores_data, original_filename)

        # Guardar los datos procesados en un CSV
        if not final_data.empty:
            csv_buffer = BytesIO()
            final_data.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
            csv_filename = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename.split('.')[0]}.csv"
            csv_buffer.seek(0)
            upload_file_to_s3(csv_buffer, csv_filename, original_filename)

    except Exception as e:
        # Manejo de errores
        error_message = f"Error al procesar el archivo Excel: {e}. Traceback: {traceback.format_exc()}"
        st.error(error_message)
        log_error_to_s3(error_message, original_filename)

# Función principal de la aplicación
def main():
    st.title("Gestión de Tableros")

    st.header("Sube un Tablero")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        process_and_upload_excel(uploaded_file, uploaded_file.name)

if __name__ == "__main__":
    main()