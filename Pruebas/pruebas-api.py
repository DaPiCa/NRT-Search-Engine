import requests

# URL de la API
url = "0.0.0.0:1212"

# Texto a traducir
text = "Hola, me llamo Juan"
from_ = "es"
to_ = "en"

# Realizamos la petición GET
response = requests.get(url, params={"text": text, "from_lang": from_, "to_lang": to_})
print(response.json())