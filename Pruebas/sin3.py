import spacy

nlp = spacy.load("es_core_news_sm")

texto = "Vivo en la Av. Libertador."

doc = nlp(texto)

for token in doc:
    if token.text == "Av.":
        print(token.nbor(-1).text + " " + token.text + " " + token.nbor(1).text)
