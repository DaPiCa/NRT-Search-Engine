FROM python:3.8-slim

COPY app.py .
COPY requirements.txt .
COPY models models
COPY core core
COPY spacy_models spacy_models

RUN pip install -r requirements.txt
RUN pip install spacy_models/de_core_news_sm-3.6.0-py3-none-any.whl
RUN pip install spacy_models/en_core_web_sm-3.6.0-py3-none-any.whl
RUN pip install spacy_models/es_core_news_sm-3.6.0-py3-none-any.whl
RUN pip install spacy_models/fr_core_news_sm-3.6.0-py3-none-any.whl
RUN pip install spacy_models/it_core_news_sm-3.6.0-py3-none-any.whl
RUN pip install spacy_models/pt_core_news_sm-3.6.0-py3-none-any.whl

ARG NLP_PORT
ENV NLP_PORT=${NLP_PORT}

HEALTHCHECK --interval=5s --timeout=3s \
  CMD curl -f http://localhost:$NLP_PORT/ || exit 1

CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port $NLP_PORT"]