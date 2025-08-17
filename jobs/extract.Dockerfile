# jobs/extract.Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Instala dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código
COPY src/ src/
COPY data/ data/

# (Opcional) Copia los flows Prefect o main
COPY src/data/extract.py .
ENV PYTHONPATH=/app

CMD ["python", "src/data/extract.py"]
