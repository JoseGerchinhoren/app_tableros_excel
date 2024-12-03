import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import pytz
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

# Función para extraer área y CUIL de las columnas
def extract_area_cuil_from_columns(sheet_data):
    for col_name in sheet_data.columns:
        if isinstance(col_name, str) and "_" in col_name:
            parts = col_name.split("_")
            if len(parts) >= 3:
                return parts[1], parts[2]
    return None, None

# Función para contar filas hasta encontrar una vacía
def count_rows_until_empty(data, column_name="Indicadores de Gestion"):
    try:
        header_index = data[data.iloc[:, 4] == column_name].index[0]
        relevant_rows = data.iloc[header_index + 1:, 4]
        return relevant_rows.isna().idxmax() - (header_index + 1)
    except Exception as e:
        st.error(f"Error contando filas hasta vacío: {e}")
        return 0

# Función para limpiar y reestructurar datos
def clean_and_restructure_until_empty(data, area, cuil):
    try:
        header_row = data[data.iloc[:, 0] == 'Referente Informacion'].index[0]
        rows_to_process = count_rows_until_empty(data, "Indicadores de Gestion")

        data.columns = data.iloc[header_row]
        data = data.iloc[header_row + 1:header_row + 1 + rows_to_process].reset_index(drop=True)

        column_mapping = {
            'Referente Informacion': 'Referente Informacion',
            'Solicitud de Informacion': 'Solicitud de Informacion',
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
            'Observacion': 'Observacion',
            'Vigente': 'Vigente',
            'Ultima Fecha de Actualización': 'Ultima Fecha de Actualización',
            'Lider Revisor': 'Lider Revisor',
            'Comentario': 'Comentario'
        }
        data = data.rename(columns=column_mapping)

        data['Área de influencia'] = area
        data['CUIL'] = cuil

        desired_columns = [
            'Área de influencia', 'CUIL', 'Referente Informacion', 'Solicitud de Informacion',
            'Tipo Indicador', 'Tipo Dato', 'Indicadores de Gestion', 'Ponderacion',
            'Objetivo Aceptable (70%)', 'Objetivo Muy Bueno (90%)', 'Objetivo Excelente (120%)',
            'Resultado', '% Logro', 'Calificación', 'Observacion', 'Vigente',
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
        sheet_data = excel_data.parse(sheet_name)
        area, cuil = extract_area_cuil_from_columns(sheet_data)
        if area and cuil:
            processed_data = clean_and_restructure_until_empty(sheet_data, area, cuil)
            final_data = pd.concat([final_data, processed_data], ignore_index=True)
    return final_data

# Función para procesar y subir el Excel
def process_and_upload_excel(file, original_filename):
    try:
        excel_data = pd.ExcelFile(file)
        cleaned_df = process_sheets_until_empty(excel_data)

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
    st.title("Subir Archivos Excel a S3 con Formato Personalizado")

    st.header("Sube tu archivo Excel")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        process_and_upload_excel(uploaded_file, uploaded_file.name)

if __name__ == "__main__":
    main()
