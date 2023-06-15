FROM python:3.8-slim

COPY app.py .
COPY requirements.txt .
COPY models models
COPY es_core_news_md-3.5.0-py3-none-any.whl .

RUN pip install -r requirements.txt
# RUN pip install --upgrade pip
RUN pip install es_core_news_md-3.5.0-py3-none-any.whl

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8009"]