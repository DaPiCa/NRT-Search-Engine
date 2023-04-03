FROM pypy:3.9-7.3.11-slim

COPY app.py .
COPY requirements.txt .

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

CMD ["pypy3", "app.py"]