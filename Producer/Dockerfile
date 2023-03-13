FROM pypy:slim-bullseye

COPY db_watcher.py .
COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*
RUN pip install -r requirements.txt

CMD ["pypy3", "db_watcher.py"]