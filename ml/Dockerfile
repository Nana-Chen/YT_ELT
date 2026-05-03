FROM python:3.11-slim

WORKDIR /app

COPY ml/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ml /app/ml

EXPOSE 8080

CMD ["uvicorn", "ml.src.app:app", "--host", "0.0.0.0", "--port", "8080"]
