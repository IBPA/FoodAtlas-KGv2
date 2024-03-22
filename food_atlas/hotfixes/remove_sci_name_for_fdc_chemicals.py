import pandas as pd


if __name__ == '__main__':
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
    ).set_index('foodatlas_id')
    entities['scientific_name'] = entities['scientific_name'].fillna('')

    entities_new = entities.copy()
    entities_chemical = entities.loc[entities['entity_type'] == 'chemical']
    entities_chemical = entities_chemical.loc[
        entities['external_ids'].apply(lambda x: 'pubchem_cid' not in x)
    ]
    entities_new.loc[entities_chemical.index, 'scientific_name'] = ''

    print((entities_new['scientific_name'] != entities['scientific_name']).sum())
    print(entities_new.to_csv("entities.tsv", sep='\t'))
