import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
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
def upload_file_to_s3(file, filename):
    try:
        s3.upload_fileobj(file, bucket_name, filename)
        st.success(f"Archivo '{filename}' subido exitosamente.")
    except Exception as e:
        st.error(f"Error al subir el archivo: {e}")

# Función para guardar errores en un archivo log en S3
def log_error_to_s3(error_message, filename):
    try:
        log_filename = "Errores.csv"
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
    pattern = r"^\d{2}-\d{2}-\d{4}\+.+$"
    return re.match(pattern, filename)

# Verificar formato de la celda A1
def validate_a1_format(cell_value):
    if not isinstance(cell_value, str):
        return False
    pattern = r"^[^_]+_[^_]+_[0-9]{11}$"
    return re.match(pattern, cell_value)

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

# Función para verificar estructura interna de cada hoja
def verify_sheet_structure(sheet_data, sheet_name, filename):
    if sheet_data.empty or sheet_data.shape[1] < 1:
        error_message = f"Error: La hoja '{sheet_name}' está vacía o no tiene suficientes columnas."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False
    cell_a1 = sheet_data.iloc[0, 0]
    if not validate_a1_format(cell_a1):
        error_message = (f"Tablero no aceptado. Formato inválido en la celda A1 en la hoja '{sheet_name}': "
                         f"Se espera un formato como 'Gerente.Comercial_Jujuy_20301508493', en su lugar fue '{cell_a1}'.")
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False
    return True

# Función para extraer datos de la celda A1
def extract_data_from_a1(cell_value):
    if validate_a1_format(cell_value):
        parts = cell_value.split("_")
        cargo = parts[0]
        area = parts[1]
        cuil = parts[2]
        return cargo, area, cuil
    return None, None, None

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
def clean_and_restructure_until_empty(data, cargo, area, cuil, filename):
    try:
        header_row = data[data.iloc[:, 0] == 'Tipo Indicador'].index[0]
        rows_to_process = count_rows_until_empty(data, "Indicadores de Gestion")

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

        data['Cargo'] = cargo
        data['Área de influencia'] = area
        data['CUIL'] = cuil

        desired_columns = [
            'Cargo', 'Área de influencia', 'CUIL',
            'Tipo Indicador', 'Tipo Dato', 'Indicadores de Gestion', 'Ponderacion',
            'Objetivo Aceptable (70%)', 'Objetivo Muy Bueno (90%)', 'Objetivo Excelente (120%)',
            'Resultado', '% Logro', 'Calificación',
            'Ultima Fecha de Actualización', 'Lider Revisor', 'Comentario'
        ]
        return data[desired_columns]
    except Exception as e:
        error_message = f"Error al limpiar y reestructurar: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return pd.DataFrame()

# Función para procesar hojas del Excel
def process_sheets_until_empty(excel_data, filename):
    final_data = pd.DataFrame()
    for sheet_name in excel_data.sheet_names:
        sheet_data = excel_data.parse(sheet_name, header=None)
        if not verify_sheet_structure(sheet_data, sheet_name, filename):
            return pd.DataFrame(), False  # Return empty DataFrame and error state
        cell_a1 = sheet_data.iloc[0, 0]
        cargo, area, cuil = extract_data_from_a1(cell_a1)
        if cargo and area and cuil:
            processed_data = clean_and_restructure_until_empty(sheet_data, cargo, area, cuil, filename)
            if processed_data.empty:
                return pd.DataFrame(), False  # Return empty DataFrame and error state
            final_data = pd.concat([final_data, processed_data], ignore_index=True)
    return final_data, True  # Return DataFrame and success state

# Función para procesar y subir el Excel
def process_and_upload_excel(file, original_filename):
    try:
        if not validate_filename(original_filename):
            error_message = "El nombre del archivo no cumple con el formato requerido (dd-mm-aaaa+empresa)."
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        excel_data = pd.ExcelFile(file)
        cleaned_df, success = process_sheets_until_empty(excel_data, original_filename)

        if not success:
            error_message = "El archivo contiene errores en su estructura y no se cargará"
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        if cleaned_df.empty:
            error_message = "El archivo no tiene datos válidos después de la limpieza."
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        csv_buffer = BytesIO()
        cleaned_df.to_csv(csv_buffer, index=False, encoding="utf-8")

        argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        now = datetime.now(argentina_tz)
        csv_filename = f"{now.strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename.split('.')[0]}.csv"

        csv_buffer.seek(0)
        upload_file_to_s3(csv_buffer, csv_filename)
    except Exception as e:
        error_message = f"Error al procesar el archivo Excel: {e}"
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