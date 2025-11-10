FROM python:3.11-slim
WORKDIR /app
# Minimal placeholder web server for ingest-service
COPY . /app
EXPOSE 8080
CMD ["python", "-m", "http.server", "8080"]
