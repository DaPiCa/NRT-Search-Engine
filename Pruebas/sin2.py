import pruebas_sinonim as ps
import logging as lg
import logging.config as lg_conf

def list_without_prepositions(original):
    prepositions = ["a", "ante", "bajo", "cabe", "con", "contra", "de", "desde", "durante", "en", "entre", "hacia", "hasta", "mediante", "para", "por", "según", "sin", "sobre", "tras"]
    words = original.split(" ")
    new_words = []
    for word in words:
        if word.lower() not in prepositions:
            new_words.append(word)
    return new_words

if __name__=="__main__":
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
    input_word = input("Introduce un ejemplo de entrada. Se buscarán sinónimos por palabra introducida: ")
    sentence = list_without_prepositions(input_word)
    for word in sentence:
        consulta = ps.consulta(word)
        formatted = ps.elastic_formatter(consulta[0], consulta[1])
        ps.lg.info(formatted)