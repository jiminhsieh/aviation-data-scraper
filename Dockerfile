FROM python:3.12.12-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY collector.py ./
COPY schema/ ./schema/
USER 65532:65532
ENTRYPOINT ["python", "-m", "collector"]
