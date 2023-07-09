# pylint: disable=import-error
import ast
import glob
import logging as lg
import logging.config as lg_conf
import os
import pathlib
import re

import nltk
import spacy
from core import translate
from fastapi import FastAPI
from py3langid import classify

nltk.download("wordnet", quiet=False)
nltk.download("omw", quiet=False)
from nltk.corpus import wordnet

lg_conf.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
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
            "\tInstalling language model: %s -> %s",
            avaliable_languages[filename.split("_")[0]],
            avaliable_languages[filename.split("_")[1]],
        )
    except KeyError:
        lg.error("\tInstalling unsupported language model: %s", model)
    translate.package.install_from_path(pathlib.Path(model))
lg.info("Installing Spacy models")

synonims_es = spacy.load("es_core_news_sm")
synonims_en = spacy.load("en_core_web_sm")
synonims_fr = spacy.load("fr_core_news_sm")
synonims_de = spacy.load("de_core_news_sm")
synonims_it = spacy.load("it_core_news_sm")
synonims_pt = spacy.load("pt_core_news_sm")

synonims = {
    "es": synonims_es,
    "en": synonims_en,
    "fr": synonims_fr,
    "de": synonims_de,
    "it": synonims_it,
    "pt": synonims_pt,
}

lg.info("Done installing packages. Starting API...")
app = FastAPI()
lg.info("API started. Waiting for requests...")


@app.get("/")
def root():
    """
    Esta función devuelve un diccionario que indica el estado de la aplicación.

    Returns:
        dict: Un diccionario que contiene la clave "status" con el valor "ok".

    """
    return {"status": "ok"}


def formatter(string):
    """
    Esta función toma una cadena de texto y la formatea según las siguientes reglas:

    - Si la cadena contiene un guión bajo (_), lo reemplaza por un espacio y encierra la cadena entre comillas dobles.
    - Si la cadena no contiene un guión bajo (_), la devuelve sin cambios.

    Args:
        string (str): La cadena de texto a formatear.

    Returns:
        str: La cadena de texto formateada.

    """
    string = str(string)
    if "_" in string:
        string = string.replace("_", " ")
        string = f'"{string}"'
    return string


def synonym_searcher(word):
    """
    Esta función busca sinónimos de una palabra utilizando la biblioteca WordNet.

    Args:
        word (str): La palabra para la que se buscan sinónimos.

    Returns:
        tuple: Una tupla que contiene la palabra original formateada y una lista de sinónimos formateados.

    """
    synonyms_list = []
    synsets = wordnet.synsets(word)
    if synsets is not None:
        for syn in synsets:
            for lemma in syn.lemmas():
                synonyms_list.append(lemma.name())
        synonyms_list = list(set(synonyms_list))
        if word.lower() in [x.lower() for x in synonyms_list]:
            lower_list = [x.lower() for x in synonyms_list]
            index = lower_list.index(word.lower())
            synonyms_list.pop(index)
    new_list = []
    for words in synonyms_list:
        new_list.append(formatter(words))
    synonyms_list = new_list
    return (formatter(word), synonyms_list)


def elastic_formatter(word: str, synonyms_list: list) -> str:
    """
    Esta función formatea una palabra y su lista de sinónimos en una cadena de \
        texto legible para ElasticSearch.

    Args:
        word (str): La palabra original a formatear.
        synonyms (list): Una lista de sinónimos de la palabra original.

    Returns:
        str: Una cadena de texto que contiene la lista de sinónimos formateada y la palabra \
            original en un formato legible para ElasticSearch.

    """
    aux = ", ".join(synonyms_list)
    return f"{word}, {aux}"


@app.get("/synonyms")
def synonyms_(text: str):
    """
    Esta función busca sinónimos de las palabras en un texto utilizando la biblioteca Spacy y WordNet.

    Args:
        text (str): El texto para el que se buscan sinónimos.

    Returns:
        list: Una lista de diccionarios que contienen la palabra original y una lista de sinónimos.

    """
    final_list = []
    new_text_dic = ast.literal_eval(text)
    original_lang = identify_lang(new_text_dic)
    if original_lang in synonims.keys():
        for _, value in new_text_dic.items():
            doc = synonims[original_lang](value)
            for token in doc:
                if (
                    (token.pos_ == "PROPN" and token.dep_ == "obl")
                    or token.pos_ == "VERB"
                    or token.pos_ == "NOUN"
                ):
                    syn = synonym_searcher(token.lemma_)
                    if syn[1]:
                        final_list.append(elastic_formatter(syn[0], syn[1]))

    return final_list


@app.get("/avaliableLanguages")
def avaliable_languages_f():
    """
    Esta función devuelve una lista de los lenguajes disponibles en la aplicación.

    Returns:
        dict: Un diccionario que contiene los lenguajes disponibles.

    """
    return avaliable_languages


@app.get("/translateAll")
def translate_all(text: str, from_lang: str) -> dict or None:
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

    # Itera sobre todos los idiomas disponibles
    for available_lang in avaliable_languages:
        # Omite el idioma fuente
        if available_lang != from_lang:
            translated_text[available_lang] = {}
            for key, value in new_text_dic.items():
                # Verifica si el valor es una cadena y contiene letras
                if isinstance(value, str) and re.search(r"[a-zA-Z]", value):
                    # Traduce el valor al idioma actual
                    translation = translate.translate(value, from_lang, available_lang)
                    # Agrega el valor traducido al diccionario de texto traducido
                    translated_text[available_lang][key] = translation

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
    lg.debug("Received request to identify language of %s", text)
    text_dic: dict = ast.literal_eval(text)
    languages = []
    for value in text_dic.values():
        lg.debug("\tIdentifying language of %s", value)
        if isinstance(value, str) and re.search(
            r"[a-zA-Z]", value
        ):  # Filtrar solo valores con letras
            language = classify(value)[0]
            lg.debug("\t\tIdentified language: %s for %s", language, value)
            languages.append(language)
    # Return the most common language
    lg.debug("Languages identified: %s", languages)
    lg.debug("Identified language: %s", max(set(languages), key=languages.count))
    return max(set(languages), key=languages.count)
