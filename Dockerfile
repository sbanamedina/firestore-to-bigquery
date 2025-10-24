# Usa una imagen oficial de Python como base
FROM python:3.11-slim

# Configura el directorio de trabajo
WORKDIR /app

# Copia los archivos necesarios
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expone el puerto (para Cloud Run)
EXPOSE 8080

# Usa el functions framework para servir la función
CMD ["functions-framework", "--target=export_firestore_to_bigquery", "--port=8080"]

