import pandas as pd


entities = pd.read_csv("outputs/kg/entities.tsv", sep="\t")
elements = pd.read_csv("food_atlas/hotfixes/data/PubChemElements_all.csv")

element_names = elements['Name'].str.lower()

def update_common_name(row):
    if row['scientific_name'] in element_names.tolist() + ['molecular nitrogen']:
        row['common_name'] = row['scientific_name']

    return row

entities = entities.apply(update_common_name, axis=1)
entities.to_csv("entities.tsv", sep='\t')
