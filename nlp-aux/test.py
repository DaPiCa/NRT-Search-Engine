import argostranslate.package
import argostranslate.translate
import pathlib
import glob
import logging as lg
import time
import os

avaliable_languages = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese",
}

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

# Create a translator
translator = argostranslate.translate.Translator("./models/en_es.argosmodel")

# Translate a string
t0 = time.time()
translation = translator.translate("Hello world!")
t1 = time.time()
lg.info(translation)
lg.info(f"Translation took {t1-t0} seconds")

# Compare using translate.translate
t0 = time.time()
translation = argostranslate.translate.translate("en", "es", "Hello world!")
t1 = time.time()
lg.info(translation)
lg.info(f"Translation took {t1-t0} seconds")