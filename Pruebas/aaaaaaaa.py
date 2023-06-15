a = {
    "text": "{'customerNumber': '103', 'customerName': 'Atelier graphique', 'contactLastName': 'Schmitt', 'contactFirstName': 'Carine ', 'phone': '40.32.2555', 'addressLine1': '54, rue Royale', 'city': 'Nantes', 'postalCode': '44000', 'country': 'France', 'salesRepEmployeeNumber': '1370', 'creditLimit': \"Decimal('21000.00')\"}"
}

import requests

r = requests.get("http://localhost:8009/detectLanguage", params=a)
print(r.text)