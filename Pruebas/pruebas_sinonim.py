import nltk
import time
import random
import logging as lg
import logging.config as lg_conf
nltk.download("omw", quiet=True)
from nltk.corpus import wordnet


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


def stats():
    words = list(wordnet.words(lang="spa"))
    SAMPLE = 100
    random_words = random.sample(words, SAMPLE)
    t0 = time.time()
    for word in random_words:
        con = consulta(word)
        lg.info(elastic_formatter(con[0], con[1]))
    t1 = time.time()
    lg.debug(f"\n\n\t{SAMPLE} random words in {t1-t0} seconds")


def consulta(word):
    return synonym_searcher(word)


if __name__ == "__main__":
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
    option = None
    while option == None or option not in ["1", "2", "3"]:
        lg.info("Insert your option:")
        lg.info("1. Search a word")
        lg.info("2. Stats")
        lg.info("3. Search infinite")
        option = input()
    if option == "1":
        lg.info("Insert the word you want to search:")
        word = input()
        cons = consulta(word)
        lg.info(elastic_formatter(cons[0], cons[1]))
    elif option == "2":
        stats()
    elif option == "3":
        while True:
            lg.info("Insert the word you want to search:")
            word = input()
            cons = consulta(word)
            lg.info(elastic_formatter(cons[0], cons[1]))
