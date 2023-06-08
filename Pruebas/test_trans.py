from decimal import Decimal
from translate import Translator
from langdetect import detect


def translate_data(data: dict) -> list:
    avaliable_languages = ["es", "en", "it", "fr", "de", "pt"]
    original_lang = identify_lang(str(data))
    translated_data = []
    translated_data.append({"lang": original_lang, "data": data})
    for lang in avaliable_languages:
        if lang != original_lang:
            translated_data.append({"lang": lang, "data": translate_to(lang, data)})
    return translated_data


def translate_to(language: str, data: dict) -> dict:
    """
    A helper function to translate the values of a dictionary from any lenguage to the
    specified language.

    Args:
        language (str): A string representing the language to translate to.
        data (dict): A dictionary object representing the data to translate.
    """
    translator = Translator(to_lang=language)
    for key, value in data.items():
        # Check if is instance of str
        if value is not None and value != "":
            data[key] = translator.translate(value)
    return data


def identify_lang(text: str) -> str:
    """
    A helper function to identify the language of a given text.

    Args:
        text (str): A string representing the text to identify the language of.

    Returns:
        str: A string representing the language of the given text.
    """
    return detect(text)


example = {
    "customerNumber": "103",
    "customerName": "Atelier graphique",
    "contactLastName": "Schmitt",
    "contactFirstName": "Carine ",
    "phone": "40.32.2555",
    "addressLine1": "54, rue Royale",
    "city": "Nantes",
    "postalCode": "44000",
    "country": "France",
    "salesRepEmployeeNumber": "1370",
    "creditLimit": 'Decimal("21000.00")',
}

print(translate_data(example))