import requests


url = "http://localhost:8009/translate"
params = {
    "text": "Hola, me llamo Juan",
    "from_lang": "es",
    "to_lang": "en",
}
response = requests.get(url, params=params)
print(response.text)
print(response.json()["translatedText"])