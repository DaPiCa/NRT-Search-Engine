FROM python:3.8-slim

COPY app.py .
COPY requirements.txt .
COPY models models
COPY core core
COPY es_core_news_md-3.5.0-py3-none-any.whl .

RUN pip install -r requirements.txt
RUN pip install es_core_news_md-3.5.0-py3-none-any.whl

ARG NLP_PORT
ENV NLP_PORT=${NLP_PORT}

HEALTHCHECK --interval=5s --timeout=3s \
  CMD curl -f http://localhost:$NLP_PORT/ || exit 1

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port $NLP_PORT"]