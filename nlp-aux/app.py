import ast
import glob
import json
import logging as lg
import logging.config as lg_conf
import os
import pathlib
import re

import argostranslate.package
import argostranslate.translate
import nltk
import spacy
from fastapi import FastAPI
from langid import classify

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
    level=lg.DEBUG,
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
    for syn in wordnet.synsets(word, lang="spa"):
        for l in syn.lemmas(lang="spa"):
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


@app.get("/translateAll")
def translateAll(text: str, from_lang: str):
    lg.debug(
        f"Received request to translate {text} from {from_lang} ({avaliable_languages[from_lang]}) to all languages"
    )
    if from_lang not in avaliable_languages.keys():
        lg.error(f"Language {from_lang} not supported")
        return None
    new_text_dic = ast.literal_eval(text)
    translated_text = {}
    for available_lang in avaliable_languages.keys():
        if available_lang != from_lang:
            lg.debug(
                f"\tTranslating to {available_lang} ({avaliable_languages[available_lang]})"
            )
            translated_text[available_lang] = {}
            for key, value in new_text_dic.items():
                if isinstance(value, str) and re.search(r"[a-zA-Z]", value):
                    lg.debug(f"\t\tTranslating {value}")
                    if available_lang == "en" or from_lang == "en":
                        translation = argostranslate.translate.translate(
                            value, from_lang, available_lang
                        )
                    else:
                        translation = argostranslate.translate.translate(
                            value, from_lang, "en"
                        )
                        translation = argostranslate.translate.translate(
                            translation, "en", available_lang
                        )
                    translated_text[available_lang][key] = translation
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
