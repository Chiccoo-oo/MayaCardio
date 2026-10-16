FROM python:3.11-slim

WORKDIR /app

# Zero external pip dependencies required - standard library only
COPY . /app

EXPOSE 8000

VOLUME ["/app/data"]

ENV MAYA_CLOUD_DB=/app/data/maya_cloud.db

CMD ["python3", "cloud/server.py", "--port", "8000", "--db", "/app/data/maya_cloud.db"]
