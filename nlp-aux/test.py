import ast
import glob
import json
import logging as lg
import logging.config as lg_conf
import os
import pathlib
import re

import argostranslate.translate
import argostranslate.package
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
    argostranslate.package.install_from_path(pathlib.Path(model))
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


def healthcheck():
    return "OK"


def avaliableLanguages():
    return avaliable_languages


import json
import re
from concurrent.futures import ThreadPoolExecutor


def translateAll(text: str, from_lang: str) -> dict or None:
    """
    Translates a dictionary of text from one language to all other available languages.

    Args:
        text (str): A dictionary of text to be translated.
        from_lang (str): The language code of the source language.

    Returns:
        dict: A dictionary of translated text for each available language.
    """
    # Check if the source language is available
    if from_lang not in avaliable_languages:
        return None

    # Convert the input text to a dictionary
    new_text_dic = ast.literal_eval(text)

    # Create an empty dictionary to store the translated text
    translated_text = {}

    # Create a ThreadPoolExecutor to perform the translations in parallel
    with ThreadPoolExecutor() as executor:
        # Create a list of futures for each available language
        futures = [
            executor.submit(translate_values, new_text_dic, from_lang, available_lang)
            for available_lang in avaliable_languages
            if available_lang != from_lang
        ]

        # Wait for all futures to complete and get the results
        for future in futures:
            available_lang, translated_values = future.result()
            translated_text[available_lang] = translated_values

    # Return the dictionary of translated text
    return translated_text

def translate_values(text_dic, from_lang, to_lang):
    """
    A helper function to translate the values of a dictionary from one language to another.

    Args:
        text_dic (dict): A dictionary of text to be translated.
        from_lang (str): The language code of the source language.
        to_lang (str): The language code of the target language.

    Returns:
        tuple: A tuple containing the target language code and a dictionary of translated text.
    """
    # Create a new translator object for each thread to avoid thread-safety issues

    # Create an empty dictionary for the translated text in the current language
    translated_text = {}

    # Iterate over all keys and values in the input dictionary
    for key, value in text_dic.items():
        # Check if the value is a string and contains letters
        if isinstance(value, str) and re.search(r"[a-zA-Z]", value):
            # Translate the value to the current language
            translation = argostranslate.translate.translate(value, from_lang, to_lang)

            # Add the translated value to the dictionary of translated text
            translated_text[key] = translation

    # Return the target language code and the dictionary of translated text
    return to_lang, translated_text


import json
import re
from collections import Counter


def identify_lang(text: str) -> str:
    """
    A helper function to identify the language of a given text.

    Args:
        text (str): A string representing the text to identify the language of.

    Returns:
        str: A string representing the language of the given text.
    """
    # Convert the text to a dictionary
    text_dic = ast.literal_eval(text)

    # Filter the values that contain letters and apply classify to each value
    languages = Counter(map(lambda value: classify(value), filter(lambda value: isinstance(value, str) and re.search(r"[a-zA-Z]", value), text_dic.values())))

    # Return the most common language
    return languages.most_common(1)[0][0][0]



query = str(
    {
        "productCode": "S700_3962",
        "productName": "The Queen Mary",
        "productLine": "Ships",
        "productScale": "1: 700",
        "productVendor": "Welly Diecast Productions",
        "productDescription": "Exact replica. Wood and Metal. Many extras including rigging, long boats, pilot house, anchors, etc. Comes with three masts, all square-rigged.",
        "quantityInStock": 5088,
        "buyPrice": "Decimal('53.63')",
        "MSRP": "Decimal('99.31')",
    }
)

import time

from viztracer import VizTracer

tracer = VizTracer()
t0 = time.time()
tracer.start()
original_lang = identify_lang(query)
lg.info(f"Identified language: {original_lang}")
res = translateAll(query, original_lang)
lg.info(f"Translated text: {res}")
t1 = time.time()
tracer.stop()
tracer.save("trace.html")
lg.info(f"Translated in {t1-t0} seconds")