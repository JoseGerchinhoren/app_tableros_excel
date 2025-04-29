import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import pytz
import re
from config import cargar_configuracion

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
        return True
    except Exception as e:
        st.error(f"Error al subir el archivo: {e}")
        return False

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
    if not re.match(pattern, filename):
        return False
    return True

# Función para validar la fecha del archivo
def validate_file_date(filename):
    try:
        file_date_str = filename.split('+')[0]
        file_date = datetime.strptime(file_date_str, '%d-%m-%Y')
        now = datetime.now()
        current_month = now.month
        current_year = now.year

        # Check if the file date is from the current month and year
        if file_date.year == current_year and file_date.month == current_month:
            return True

        return False
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
def validate_form_cells(sheet_data, sheet_name, filename, is_vendedores):
    try:
        if is_vendedores:
            # Omitir validación de B1 si la hoja es "Resumen RRHH"
            if sheet_name == "Resumen RRHH":
                return True

            # Validar formulario de vendedores
            if pd.isna(sheet_data.at[0, 1]) or not re.match(r"^\d{11}$", str(sheet_data.at[0, 1])):
                error_message = f"Error: La celda B1 en la hoja '{sheet_name}' debe contener un CUIL válido (11 dígitos)."
                st.error(error_message)
                log_error_to_s3(error_message, filename)
                return False
        else:
            # Validar formulario de no vendedores
            required_cells = ['B1', 'B2', 'B3', 'B4']
            for cell in required_cells:
                if pd.isna(sheet_data.at[int(cell[1]) - 1, 1]):
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
def process_sheets_until_empty(excel_data, filename, upload_datetime, is_vendedores):
    final_data = pd.DataFrame()
    aceleradores_data = pd.DataFrame()
    resumen_rrhh_data = None
    leader_name = extract_leader_name(filename)
    fecha, sucursal = extract_date_and_sucursal(filename)
    dataframes = []

    try:
        for sheet_name in excel_data.sheet_names:
            sheet_data = excel_data.parse(sheet_name, header=None)

            # Si es la hoja "Resumen RRHH" y es un tablero de vendedores, validar columnas requeridas
            if is_vendedores and sheet_name == "Resumen RRHH":
                valid_rrhh, resumen_rrhh_data = validate_resumen_rrhh_sheet(excel_data, filename)
                if not valid_rrhh:
                    return pd.DataFrame(), pd.DataFrame(), None, False  # Return empty DataFrames and error state
                continue  # No procesar más esta hoja

            if not verify_sheet_structure(sheet_data, sheet_name, filename):
                return pd.DataFrame(), pd.DataFrame(), None, False  # Return empty DataFrames and error state

            if not validate_form_cells(sheet_data, sheet_name, filename, is_vendedores):
                return pd.DataFrame(), pd.DataFrame(), None, False  # Return empty DataFrames and error state

            if is_vendedores:
                # Procesar hojas de vendedores
                processed_data, aceleradores_sheet_data = process_vendedores_tablero(sheet_data, filename, upload_datetime, sheet_name)
                aceleradores_data = pd.concat([aceleradores_data, aceleradores_sheet_data], ignore_index=True)
            else:
                # Procesar hojas de no vendedores
                cargo, cuil, segmento, area_influencia, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo = extract_data_from_form(sheet_data)
                if not all([cargo, cuil, segmento, area_influencia]):
                    error_message = f"Error: El formulario en la hoja '{sheet_name}' no contiene todos los datos requeridos."
                    st.error(error_message)
                    log_error_to_s3(error_message, filename)
                    return pd.DataFrame(), pd.DataFrame(), None, False

                processed_data = clean_and_restructure_until_empty(
                    sheet_data, cargo, cuil, segmento, area_influencia, leader_name, fecha, sucursal, filename,
                    upload_datetime, sheet_name, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo
                )

            if processed_data.empty:
                return pd.DataFrame(), pd.DataFrame(), None, False  # Return empty DataFrames and error state

            dataframes.append(processed_data)
            final_data = pd.concat([final_data, processed_data], ignore_index=True)

        return final_data, aceleradores_data, resumen_rrhh_data, True  # Return DataFrames and success state
    except Exception as e:
        error_message = f"Error al procesar las hojas del archivo: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return pd.DataFrame(), pd.DataFrame(), None, False

# Función para determinar si el tablero es "Ajuste" o "Normal"
def determine_tablero_type(fecha, upload_datetime):
    fecha_tablero = datetime.strptime(fecha, '%d-%m-%Y')
    ajuste_fecha = datetime.strptime('23/04/2025', '%d/%m/%Y')  # Ingresar la fecha de ajuste aquí
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

# Función para procesar y subir el Excel
def process_and_upload_excel(file, original_filename):
    try:
        if not validate_filename(original_filename):
            error_message = "El nombre del archivo no cumple con el formato requerido (dd-mm-aaaa+empresa+nombre lider.xlsx)."
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        if not validate_file_date(original_filename):
            error_message = "La fecha del nombre del archivo solo puede ser del mes  al actual."
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        is_vendedores = is_vendedores_tablero(original_filename)
        excel_data = pd.ExcelFile(file)
        argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        now = datetime.now(argentina_tz)
        upload_datetime = now.strftime('%Y-%m-%d_%H-%M-%S')

        # Extraer el nombre del líder desde el nombre del archivo
        leader_name = extract_leader_name(original_filename)

        # Procesar las hojas del archivo
        cleaned_df, aceleradores_data, resumen_rrhh_data, success = process_sheets_until_empty(
            excel_data, original_filename, upload_datetime, is_vendedores
        )

        if not success:
            error_message = "El archivo contiene errores en su estructura y no se cargará"
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        # Guardar la tabla "Resumen RRHH" solo si no hubo errores
        if is_vendedores and resumen_rrhh_data is not None:
            if save_resumen_rrhh_to_csv(resumen_rrhh_data, original_filename, upload_datetime):
                st.success(f"Archivo 'Resumen RRHH' guardado correctamente.csv'.")

        if not cleaned_df.empty:
            # Guardar el archivo principal en S3
            csv_buffer = BytesIO()
            cleaned_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
            csv_filename = f"{now.strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename.split('.')[0]}.csv"
            csv_buffer.seek(0)
            if upload_file_to_s3(csv_buffer, csv_filename, original_filename):
                st.success(f"Archivo '{original_filename}' subido exitosamente.")

        if not aceleradores_data.empty:
            save_aceleradores_to_csv(aceleradores_data, original_filename, upload_datetime)
    except Exception as e:
        error_message = f"Error al procesar el archivo Excel: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, original_filename)

def is_vendedores_tablero(filename):
    try:
        parts = filename.split('+')
        if len(parts) > 1 and "Vendedores" in parts[1]:
            return True
        return False
    except Exception as e:
        st.error(f"Error al determinar el tipo de tablero: {e}")
        return False

def validate_resumen_rrhh_sheet(excel_data, filename):
    try:
        if "Resumen RRHH" not in excel_data.sheet_names:
            error_message = "Error: La hoja 'Resumen RRHH' no está presente en el archivo."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False, None

        # Leer la hoja "Resumen RRHH" y establecer la fila 2 como encabezado
        resumen_rrhh_data = excel_data.parse("Resumen RRHH", header=1)

        # Limpiar los nombres de las columnas (eliminar espacios en blanco y caracteres invisibles)
        resumen_rrhh_data.columns = resumen_rrhh_data.columns.str.strip()

        # Validar que las columnas requeridas estén presentes
        required_columns = [
            'Sucursal', 'Vendedores', 'CUIT', 'LEGAJO', 'Total Ventas', 'Vta PPAA',
            'Descuentos PPAA', 'COMISION PPAA', '0km', 'Usados', 'Premio Convencional',
            'Comision Convencional', 'Total a liquidar'
        ]
        missing_columns = [col for col in required_columns if col not in resumen_rrhh_data.columns]
        if missing_columns:
            error_message = f"Error: Faltan las siguientes columnas en la hoja 'Resumen RRHH': {', '.join(missing_columns)}"
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False, None

        return True, resumen_rrhh_data
    except Exception as e:
        error_message = f"Error al validar la hoja 'Resumen RRHH': {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False, None

def process_vendedores_tablero(sheet_data, filename, upload_datetime, sheet_name):
    try:
        # Extraer valores de las celdas B1 (CUIL) y H2 a M2 (Indicadores)
        cuil = sheet_data.iloc[0, 1] if len(sheet_data) > 0 else None  # Celda B1
        indicadores = sheet_data.iloc[1, 7:13].values.tolist()  # Celdas H2 a M2
        Segmento = sheet_data.iloc[1, 1] if len(sheet_data) > 1 else None  # Celda B2
        
        # Crear un DataFrame para los aceleradores
        aceleradores_data = pd.DataFrame([{
            'CUIL': cuil,
            'Indicador 1': indicadores[0] if len(indicadores) > 0 else None,
            'Indicador 2': indicadores[1] if len(indicadores) > 1 else None,
            'Indicador 3': indicadores[2] if len(indicadores) > 2 else None,
            'Indicador 4': indicadores[3] if len(indicadores) > 3 else None,
            'Indicador 5': indicadores[4] if len(indicadores) > 4 else None,
            'Indicador 6': indicadores[5] if len(indicadores) > 5 else None,
        }])

        # Procesar el resto de los datos del tablero (funcionalidad existente)
        header_row = sheet_data[sheet_data.iloc[:, 0] == 'Tipo Indicador'].index
        if header_row.empty:
            error_message = f"Error: No se encontró el encabezado 'Tipo Indicador' en la hoja '{sheet_name}'."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return pd.DataFrame(), aceleradores_data

        header_row = header_row[0]
        rows_to_process = count_rows_until_empty(sheet_data, "Indicadores de Gestion")

        if rows_to_process == 0:
            error_message = "Error: No se encontraron filas válidas después del encabezado."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return pd.DataFrame(), aceleradores_data

        # Configurar encabezados y recortar filas relevantes
        sheet_data.columns = sheet_data.iloc[header_row]
        sheet_data = sheet_data.iloc[header_row + 1:header_row + 1 + rows_to_process].reset_index(drop=True)

        # Validar columnas requeridas
        valid_columns, missing_columns = validate_required_columns(sheet_data)
        if not valid_columns:
            error_message = f"Error: Faltan las siguientes columnas requeridas: {', '.join(missing_columns)}"
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return pd.DataFrame(), aceleradores_data

        # Validar ponderaciones
        if not validate_ponderacion(sheet_data, filename):
            return pd.DataFrame(), aceleradores_data

        if not validate_ponderacion_sum(sheet_data, filename, sheet_name):
            return pd.DataFrame(), aceleradores_data

        # Extraer datos adicionales del archivo
        fecha, sucursal = extract_date_and_sucursal(filename)
        leader_name = extract_leader_name(filename)

        # Agregar columnas adicionales específicas para vendedores
        sheet_data['Cargo'] = "Vendedor"  # Valor de la celda B2
        sheet_data['CUIL'] = cuil  # Celda B1
        sheet_data['Segmento'] = Segmento
        sheet_data['Área de influencia'] = ""
        sheet_data['Nombre Lider'] = leader_name
        sheet_data['Fecha_Nombre_Archivo'] = fecha
        sheet_data['Sucursal'] = sucursal
        sheet_data['Fecha Horario Subida'] = upload_datetime

        # Reorganizar las columnas en el orden requerido
        desired_columns = [
            'Cargo', 'CUIL', 'Segmento', 'Área de influencia', 'Nombre Lider', 'Fecha_Nombre_Archivo', 'Sucursal',
            'Fecha Horario Subida', 'Tipo Indicador', 'Tipo Dato', 'Indicadores de Gestion', 'Ponderacion',
            'Objetivo Aceptable (70%)', 'Objetivo Muy Bueno (90%)', 'Objetivo Excelente (120%)',
            'Resultado', '% Logro', 'Calificación', 'Ultima Fecha de Actualización', 'Lider Revisor', 'Comentario'
        ]
        return sheet_data[desired_columns], aceleradores_data
    except Exception as e:
        error_message = f"Error al procesar el tablero de vendedores: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return pd.DataFrame(), pd.DataFrame()

def save_resumen_rrhh_to_csv(resumen_rrhh_data, original_filename, upload_datetime):
    try:
        resumen_rrhh_data = resumen_rrhh_data.dropna(how='all', subset=[
            'Sucursal', 'Vendedores', 'CUIT', 'LEGAJO', 'Total Ventas', 'Vta PPAA',
            'Descuentos PPAA', 'COMISION PPAA', '0km', 'Usados', 'Premio Convencional',
            'Comision Convencional', 'Total a liquidar'
        ])

        csv_filename = f"RRHH-{upload_datetime}_{original_filename.split('.')[0]}.csv"
        csv_buffer = BytesIO()
        resumen_rrhh_data.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        csv_buffer.seek(0)

        upload_file_to_s3(csv_buffer, csv_filename, original_filename)
        return True
    except Exception as e:
        st.error(f"Error al guardar el archivo 'Resumen RRHH': {e}")
        return False

def save_aceleradores_to_csv(aceleradores_data, original_filename, upload_datetime):
    try:
        # Crear el nombre del archivo CSV
        csv_filename = f"Aceleradores-{upload_datetime}_{original_filename.split('.')[0]}.csv"

        # Guardar el DataFrame en un archivo CSV
        csv_buffer = BytesIO()
        aceleradores_data.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        csv_buffer.seek(0)

        # Subir el archivo CSV a S3
        upload_file_to_s3(csv_buffer, csv_filename, original_filename)
        st.success(f"Archivo de aceleradores guardado correctamente'.")
    except Exception as e:
        error_message = f"Error al guardar el archivo de aceleradores: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, original_filename)

# Función principal de la aplicación
def main():
    st.title("Gestión de Tableros")

    st.header("Sube un Tablero")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        # Usa una clave única para evitar recargas múltiples
        if st.session_state.get("uploaded_file") != uploaded_file.name:
            st.session_state["uploaded_file"] = uploaded_file.name
            process_and_upload_excel(uploaded_file, uploaded_file.name)

if __name__ == "__main__":
    main()