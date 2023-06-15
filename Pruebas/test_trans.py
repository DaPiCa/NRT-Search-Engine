import re
from langid import classify

def identify_lang(text: dict) -> str:
    """
    A helper function to identify the language of a given text.

    Args:
        text (dict): A dict representing the text to identify the language of.

    Returns:
        str: A string representing the language of the given text.
    """
    # Check every value of the dict
    languages = []
    for value in text.values():
        if isinstance(value, str) and re.search(r'[a-zA-Z]', value):  # Filtrar solo valores con letras
            language = classify(value)[0]
            languages.append(language)
    # Return the most common language
    print(languages)
    return max(set(languages), key=languages.count)


example = {
    "customerNumber": "103",
    "customerName": "Atelier graphique",
    "contactLastName": "Schmitt",
    "contactFirstName": "Buenas tardes",
    "phone": "40.32.2555",
    "addressLine1": "54, rue Royale",
    "city": "Nantes",
    "postalCode": "44000",
    "country": "France",
    "salesRepEmployeeNumber": "1370",
    "creditLimit": 'Decimal("21000.00")',
}

print(identify_lang(example))
