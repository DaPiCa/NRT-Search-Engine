import spacy
import pruebas_sinonim as ps
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
        level=lg.INFO,
    )

nlp = spacy.load("es_core_news_sm")

texto = "El Sr. González vive en la Avenida de la Constitución, pero tiene más pisos en la Calle España."

doc = nlp(texto)

for token in doc:
    # Imprimimos toda la información morfológica, sintáctica y semántica que nos proporciona Spacy
    lg.debug(f"T {token.text}, Lema: {token.lemma_}, POS: {token.pos_}, Tag: {token.tag_}, Dep: {token.dep_}, Shape: {token.shape_}, Alpha: {token.is_alpha}, Stop: {token.is_stop}")
    # Identificamos propn y verb
    if (token.pos_ == "PROPN" and token.dep_=="obl") or token.pos_ == "VERB" or token.pos_ == "NOUN":
        lg.debug("Token: %s", token.lemma_)
        aux = ps.consulta(token.lemma_)
        lg.debug("Consulta: %s", aux)
        if aux[1]:
            formatted = ps.elastic_formatter(aux[0], aux[1])
            lg.info(formatted)