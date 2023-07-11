import nltk
import logging as lg
import logging.config as lg_conf

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

nltk.download("wordnet", quiet=False)
nltk.download("omw", quiet=False)
from nltk.corpus import wordnet

syn = {
     "en": "eng",
        "es": "spa",
        "fr": "fra",
        "it": "ita",
        "pt": "por",

}

lg.info(wordnet.langs())

cadena = input("Introduce una palabra: ")
synsets = wordnet.synsets(cadena)
if synsets is not None:
        for syn in synsets:
            for l in syn.lemmas():
                lg.info(l.name())