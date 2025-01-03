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
        st.success(f"Archivo '{filename}' subido exitosamente a S3.")
    except Exception as e:
        st.error(f"Error al subir el archivo a S3: {e}")

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

# Función para verificar estructura interna de cada hoja
def verify_sheet_structure(sheet_data, sheet_name):
    if sheet_data.empty or sheet_data.shape[1] < 1:
        st.error(f"Error: La hoja '{sheet_name}' está vacía o no tiene suficientes columnas.")
        return False
    cell_a1 = sheet_data.iloc[0, 0]
    if not validate_a1_format(cell_a1):
        st.error(f"Tablero no aceptado. Formato inválido en la celda A1 en la hoja '{sheet_name}': "
                 f"Se espera un formato como 'Gerente.Comercial_Jujuy_20301508493', en su lugar fue '{cell_a1}'.")
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
def clean_and_restructure_until_empty(data, cargo, area, cuil):
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
        st.error(f"Error al limpiar y reestructurar: {e}")
        return pd.DataFrame()

# Función para procesar hojas del Excel
def process_sheets_until_empty(excel_data):
    final_data = pd.DataFrame()
    for sheet_name in excel_data.sheet_names:
        sheet_data = excel_data.parse(sheet_name, header=None)
        if not verify_sheet_structure(sheet_data, sheet_name):
            return pd.DataFrame(), False  # Return empty DataFrame and error state
        cell_a1 = sheet_data.iloc[0, 0]
        cargo, area, cuil = extract_data_from_a1(cell_a1)
        if cargo and area and cuil:
            processed_data = clean_and_restructure_until_empty(sheet_data, cargo, area, cuil)
            final_data = pd.concat([final_data, processed_data], ignore_index=True)
    return final_data, True  # Return DataFrame and success state

# Función para procesar y subir el Excel
def process_and_upload_excel(file, original_filename):
    try:
        if not validate_filename(original_filename):
            st.error("El nombre del archivo no cumple con el formato requerido (dd-mm-aaaa+empresa).")
            return

        excel_data = pd.ExcelFile(file)
        cleaned_df, success = process_sheets_until_empty(excel_data)

        if not success:
            st.error("El archivo contiene errores en su estructura y no se cargará en S3.")
            return

        if cleaned_df.empty:
            st.error("El archivo no tiene datos válidos después de la limpieza.")
            return

        csv_buffer = BytesIO()
        cleaned_df.to_csv(csv_buffer, index=False, encoding="utf-8")

        argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        now = datetime.now(argentina_tz)
        csv_filename = f"{now.strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename.split('.')[0]}.csv"

        csv_buffer.seek(0)
        upload_file_to_s3(csv_buffer, csv_filename)
    except Exception as e:
        st.error(f"Error al procesar el archivo Excel: {e}")

# Función principal de la aplicación
def main():
    st.title("Gestión de Tableros")

    st.header("Sube un Tablero")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        process_and_upload_excel(uploaded_file, uploaded_file.name)

if __name__ == "__main__":
    main()