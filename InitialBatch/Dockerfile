FROM pypy:3.9-7.3.11-slim

COPY main.py .
COPY requirements.txt .

# RUN pip install --upgrade pip

RUN pip install -r requirements.txt

CMD ["pypy3", "main.py"]