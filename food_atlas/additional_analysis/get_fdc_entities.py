from ast import literal_eval

import pandas as pd


if __name__ == '__main__':
    entities = pd.read_csv(
        "outputs/kg/initialization/entities.tsv",
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    ).set_index('foodatlas_id')

    chemical_entities = entities[entities['entity_type'] == 'chemical'].copy()
    chemical_entities['pubchem_cid'] = chemical_entities['external_ids'].apply(
        lambda x: x.get('pubchem_cid', None)
    ).astype('Int64')
    chemical_entities['fdc_nutrient_ids'] = chemical_entities['external_ids'].apply(
        lambda x: x.get('fdc_nutrient_ids', None)
    )

    chemical_entities['fdc_nutrient_ids'] \
        = chemical_entities['fdc_nutrient_ids'].apply(
        lambda x: x[0] if len(x) == 1 else x
    )
    chemical_entities_mult = chemical_entities[
        chemical_entities['fdc_nutrient_ids'].apply(
            lambda x: True if type(x) == list else False
        )
    ]
    for i, row in chemical_entities_mult.iterrows():
        for fdc_nutrient_id in row['fdc_nutrient_ids']:
            chemical_entities.loc[
                chemical_entities['fdc_nutrient_ids'] == fdc_nutrient_id,
                'fdc_nutrient_ids'
            ] = '_'.join([str(x) for x in row['fdc_nutrient_ids']])
        chemical_entities.loc[row.name, 'fdc_nutrient_ids'] \
            = '_'.join([str(x) for x in row['fdc_nutrient_ids']])

    result_dfs = []
    chemical_entities.groupby('fdc_nutrient_ids').apply(
        lambda x: result_dfs.append(x) if len(x) > 1 else None
    )
    chemical_entities = pd.concat(result_dfs)

    chemical_names = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/nutrient.csv"
    ).set_index('id')
    chemical_entities['fdc_name'] = None
    for i, row in chemical_entities.iterrows():
        ids = [int(x) for x in (str(row['fdc_nutrient_ids']).split('_'))]
        names = chemical_names.loc[ids]['name']
        chemical_entities.loc[i, 'fdc_name'] = '; '.join(names.tolist())

    chemical_entities.to_csv("ambiguous_chemical_entities.tsv", sep='\t')
