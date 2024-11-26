import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import pytz  # Biblioteca para manejar zonas horarias
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

# Función para convertir Excel a CSV y subir a S3
def process_and_upload_excel(file, original_filename):
    try:
        # Leer el archivo Excel como DataFrame
        df = pd.read_excel(file, engine="openpyxl")
        
        # Convertir a CSV
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False, encoding="utf-8")
        
        # Generar un nombre único para el archivo CSV con horario de Argentina
        argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        now = datetime.now(argentina_tz)
        csv_filename = f"{now.strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename.split('.')[0]}.csv"

        # Subir el archivo CSV a S3
        csv_buffer.seek(0)  # Reiniciar el puntero del buffer antes de subir
        upload_file_to_s3(csv_buffer, csv_filename)
    except Exception as e:
        st.error(f"Error al procesar el archivo Excel: {e}")

# Función principal de la aplicación
def main():
    st.title("Subir Archivos Excel a S3")

    # Formulario para subir archivos
    st.header("Sube tu archivo Excel")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        # Convertir y subir el archivo Excel como CSV
        process_and_upload_excel(uploaded_file, uploaded_file.name)

if __name__ == "__main__":
    main()
