import streamlit as st
import boto3
from io import BytesIO
from datetime import datetime
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

# Función principal de la aplicación
def main():
    st.title("Subir Archivos Excel a S3")

    # Formulario para subir archivos
    st.header("Sube tu archivo Excel")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        # Asignar un nombre único al archivo
        now = datetime.now()
        filename = f"{now.strftime('%Y-%m-%d_%H-%M-%S')}_{uploaded_file.name}"

        # Subir el archivo a S3
        file_buffer = BytesIO(uploaded_file.read())
        upload_file_to_s3(file_buffer, filename)

if __name__ == "__main__":
    main()
