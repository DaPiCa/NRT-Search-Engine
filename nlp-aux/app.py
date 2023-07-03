import ast
import glob
import json
import logging as lg
import logging.config as lg_conf
import os
import pathlib
import re

import core.translate as translate
import nltk
import spacy
from fastapi import FastAPI
from py3langid import classify

nltk.download("omw", quiet=True)
from nltk.corpus import wordnet

lg_conf.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": True,
    }
)
lg.basicConfig(
    format="%(asctime)s | %(filename)s | %(levelname)s |>> %(message)s",
    level=lg.INFO,
)

avaliable_languages = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese",
}

# Download and install Argos Translate package
lg.info("Installing Argos Translate package")
models = glob.glob("models/*.argosmodel")
for model in models:
    try:
        # If Windows, replace \ with / in path
        if os.name == "nt":
            filename = model.split("\\")[-1].split(".")[0]
        else:
            filename = model.split("/")[-1].split(".")[0]
        lg.info(
            f"\tInstalling language model: {avaliable_languages[filename.split('_')[0]]} -> {avaliable_languages[filename.split('_')[1]]}"
        )
    except KeyError:
        lg.error(f"\tInstalling unsupported language model: {model}")
    translate.package.install_from_path(pathlib.Path(model))
lg.info("Installing Spacy model")
synonims = spacy.load("es_core_news_md")

lg.info("Done installing packages. Starting API...")
app = FastAPI()
lg.info("API started. Waiting for requests...")


def formatter(string):
    string = str(string)
    if "_" in string:
        string = string.replace("_", " ")
        string = f'"{string}"'
    return string


def synonym_searcher(word):
    synonyms = []
    synsets = wordnet.synsets(word)
    if synsets is not None:
        for syn in synsets:
            for l in syn.lemmas():
                synonyms.append(l.name())
        synonyms = list(set(synonyms))
        if word.lower() in [x.lower() for x in synonyms]:
            lower_list = [x.lower() for x in synonyms]
            index = lower_list.index(word.lower())
            synonyms.pop(index)
    new_list = []
    for words in synonyms:
        new_list.append(formatter(words))
    synonyms = new_list
    return (formatter(word), synonyms)


def elastic_formatter(word, synonyms):
    aux = ", ".join(synonyms)
    return f"{aux} => {word}"


@app.get("/synonyms")
def synonyms(text: str):
    final_list = []
    doc = synonims(text)
    for token in doc:
        # Imprimimos toda la información morfológica, sintáctica y semántica que nos proporciona Spacy
        lg.debug(
            f"T {token.text}, Lema: {token.lemma_}, POS: {token.pos_}, Tag: {token.tag_}, Dep: {token.dep_}, Shape: {token.shape_}, Alpha: {token.is_alpha}, Stop: {token.is_stop}"
        )
        # Identificamos propn y verb
        if (
            (token.pos_ == "PROPN" and token.dep_ == "obl")
            or token.pos_ == "VERB"
            or token.pos_ == "NOUN"
        ):
            final_list.append(elastic_formatter(*synonym_searcher(token.lemma_)))
    return final_list


@app.get("/healthcheck")
def healthcheck():
    return "OK"


@app.get("/avaliableLanguages")
def avaliableLanguages():
    return avaliable_languages


from queue import Queue
import threading


def translateText(original: dict, from_lang: str, to_lang: str, result_queue: Queue):
    translated_text = {}

    # Itera sobre todas las claves y valores en el diccionario original
    for key, value in original.items():
        # Verifica si el valor es una cadena y contiene letras
        if isinstance(value, str) and re.search(r"[a-zA-Z]", value):
            # Traduce el valor al idioma actual
            translation = translate.translate(value, from_lang, to_lang)
            # Agrega el valor traducido al diccionario de texto traducido
            translated_text[key] = translation

    result_queue.put((to_lang, translated_text))

@app.get("/translateAll")
def translateAll(text: str, from_lang: str) -> dict or None:
    """
    Translates a dictionary of text from one language to all other available languages.

    Args:
        text (str): A dictionary of text to be translated.
        from_lang (str): The language code of the source language.

    Returns:
        dict: A dictionary of translated text for each available language.
    """
    # Verifica si el idioma fuente está disponible
    if from_lang not in avaliable_languages:
        return None

    # Convierte el texto de entrada en un diccionario
    new_text_dic = ast.literal_eval(text)

    # Crea un diccionario vacío para almacenar el texto traducido
    translated_text = {}

    result_queue = Queue()

    threads = []

    # Itera sobre todos los idiomas disponibles
    for available_lang in avaliable_languages:
        # Omite el idioma fuente
        if available_lang != from_lang:
            thread = threading.Thread(target=translateText, args=(new_text_dic, from_lang, available_lang, result_queue))
            threads.append(thread)
            thread.start()

    for thread in threads:
        thread.join()

    while not result_queue.empty():
        lang, thread_translated_text = result_queue.get()
        translated_text[lang] = thread_translated_text

    # Devuelve el diccionario de texto traducido
    return translated_text


@app.get("/detectLanguage")
def identify_lang(text: str) -> str:
    """
    A helper function to identify the language of a given text.

    Args:
        text (str): A string representing the text to identify the language of.

    Returns:
        str: A string representing the language of the given text.
    """
    # Check every value of the dict
    lg.debug(f"Received request to identify language of {text}")
    text_dic: dict = ast.literal_eval(text)
    lg.debug(f"Dict: {text_dic}")
    languages = []
    for value in text_dic.values():
        lg.debug(f"\tIdentifying language of {value}")
        if isinstance(value, str) and re.search(
            r"[a-zA-Z]", value
        ):  # Filtrar solo valores con letras
            language = classify(value)[0]
            lg.debug(f"\t\tIdentified language: {language} for {value}")
            languages.append(language)
    # Return the most common language
    lg.debug(f"Languages: {languages}")
    lg.debug(f"Identified language: {max(set(languages), key=languages.count)}")
    return max(set(languages), key=languages.count)
