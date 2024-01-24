import pandas as pd


if __name__ == '__main__':
    data = pd.read_json("data/FooDB/Food.json", lines=True)

    data = data[[
        'id', 'public_id', 'name', 'name_scientific', 'itis_id', 'wikipedia_id',
        'food_group', 'food_subgroup', 'food_type', 'category', 'ncbi_taxonomy_id',
    ]]
    data = data.rename(columns={
        'public_id': 'foodb_id',
        'name': 'orig_name',
        'name_scientific': 'scientific_name',
        'ncbi_taxonomy_id': 'ncbi_taxon_id',
    })
    data = data.drop(
        columns=[
            'id',
            'wikipedia_id',
            # 'food_group',
            # 'food_subgroup',
            'food_type',
            'category',
        ]
    )
    data.to_csv('outputs/kg/foods/foodb.csv')
