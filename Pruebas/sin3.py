import spacy

# Cargamos un modelo en español
nlp = spacy.load("es_core_news_sm")

texto = "El Señor González vive en la Avenida de la Constitución, pero tiene más pisos en la Calle España."

doc = nlp(texto)

for token in doc:
    # Imprimimos toda la información morfológica, sintáctica y semántica que nos proporciona Spacy
    print(f"T {token.text}, Lema: {token.lemma_}, POS: {token.pos_}, Tag: {token.tag_}, Dep: {token.dep_}, Shape: {token.shape_}, Alpha: {token.is_alpha}, Stop: {token.is_stop}")