import argostranslate.package
import argostranslate.translate
from fastapi import FastAPI
from itertools import combinations
import logging as lg
import logging.config as lg_conf

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

avaliable_languages = [
    ("es", "en"),
    ("en", "es"),
    ("en", "it"),
    ("en", "fr"),
    ("en", "de"),
    ("en", "pt"),
    ("it", "en"),
    ("fr", "en"),
    ("de", "en"),
    ("pt", "en"),
]

# Download and install Argos Translate package
lg.info("Downloading and installing Argos Translate package")
argostranslate.package.update_package_index()
available_packages = argostranslate.package.get_available_packages()

for combination in avaliable_languages:
    package_to_install = next(
        filter(
            lambda x: x.from_code == combination[0] and x.to_code == combination[1],
            available_packages,
        )
    )
    lg.info(
        f"\tInstalling package {combination[0]}-{combination[1]}: {package_to_install}"
    )
    argostranslate.package.install_from_path(package_to_install.download())

lg.info("Done installing packages. Starting API...")
app = FastAPI()
lg.info("API started. Waiting for requests...")
# Create translator


@app.get("/")
def translate(text: str, from_lang: str, to_lang: str):
    if from_lang == to_lang:
        return {"translatedText": text}
    if (from_lang, to_lang) not in avaliable_languages:
        return {"translatedText": "Not supported"}
    translatedText = argostranslate.translate.translate(text, from_lang, to_lang)
    return {"translatedText": translatedText}
