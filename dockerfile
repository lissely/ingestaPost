FROM python:3.11-slim

WORKDIR /app

COPY ingestaPost.py /app/
COPY requirements.txt /app/
COPY .env /app/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "ingestaPost.py"]