import requests


url = "http://localhost:5000"
params = {
    "text": "Hola, me llamo Juan",
    "from_lang": "es",
    "to_lang": "en",
}
response = requests.get(url, params=params)
print(response.json()["translatedText"])