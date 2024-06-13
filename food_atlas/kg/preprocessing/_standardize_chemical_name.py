from pandarallel import pandarallel

from . import constants

pandarallel.initialize(progress_bar=True)


def standardize_chemical_name(chemical_name: str):
    for eng, greek_letters in constants.GREEK_LETTERS.items():
        for greek_letter in greek_letters:
            chemical_name = chemical_name.replace(greek_letter, eng)

    for punc_new, punc_old_list in constants.PUNCTUATIONS.items():
        for punc_old in punc_old_list:
            chemical_name = chemical_name.replace(punc_old, punc_new)

    return chemical_name
