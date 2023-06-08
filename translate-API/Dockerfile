FROM python:3.8-slim

COPY app.py .
COPY requirements.txt .

# RUN pip install --upgrade pip

RUN pip install -r requirements.txt

CMD ["python3", "app.py"]