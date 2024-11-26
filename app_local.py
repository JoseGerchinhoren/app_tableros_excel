import streamlit as st
import os
from datetime import datetime

# Configuración de directorio local
UPLOAD_FOLDER = "uploads"

# Crear el directorio local si no existe
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Función para guardar archivo localmente
def save_file_locally(file, filename):
    try:
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        with open(file_path, "wb") as f:
            f.write(file.getbuffer())
        st.success(f"Archivo '{filename}' guardado exitosamente en '{UPLOAD_FOLDER}'.")
    except Exception as e:
        st.error(f"Error al guardar el archivo: {e}")

# Función principal de la aplicación
def main():
    st.title("Prueba Local: Subir Archivos Excel")

    # Formulario para subir archivos
    st.header("Sube tu archivo Excel")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        # Asignar un nombre único al archivo
        now = datetime.now()
        filename = f"{now.strftime('%Y-%m-%d_%H-%M-%S')}_{uploaded_file.name}"

        # Guardar el archivo localmente
        save_file_locally(uploaded_file, filename)

if __name__ == "__main__":
    main()
