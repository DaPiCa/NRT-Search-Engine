import argostranslate.package
import argostranslate.translate

available_languages = ["es", "en", "it", "fr", "de", "pt"]

# Download and install Argos Translate package
argostranslate.package.update_package_index()
available_packages = argostranslate.package.get_available_packages()
tuples = []
for i in range(len(available_languages)):
    for j in range(len(available_languages)):
        if i != j:
            tuples.append((available_languages[i], available_languages[j]))

final_packages = []
for comb in tuples:
    package_to_install = None
    for package in available_packages:
        if package.from_code == comb[0] and package.to_code == comb[1]:
            final_packages.append(comb)
            package_to_install = package
            break
    if package_to_install is None:
        print("No package found for translation from " + comb[0] + " to " + comb[1])
    else:
        argostranslate.package.install_from_path(package_to_install.download())

# Translate
for lang in available_languages:
    if lang != "en":
        translatedText = argostranslate.translate.translate("Hello World", "en", lang)
        print(translatedText)

print("\n\n")
print(final_packages)
# '¡Hola Mundo!'