from ast import literal_eval
from collections import defaultdict

import pandas as pd


def parse_other_names(other_names):
    if other_names is None:
        return []

    synonyms = []
    synonyms += other_names['Synonym']
    synonyms += other_names['EquivalentName']
    synonyms += other_names['CommonName']
    synonyms += other_names['Includes']

    if 'GenbankCommonName' in other_names:
        synonyms += [other_names['GenbankCommonName']]
    if 'BlastName' in other_names:
        synonyms += [other_names['BlastName']]
    if other_names['Name']:
        for name in other_names['Name']:
            if name['ClassCDE'] == 'misspelling':
                synonyms += [name['DispName']]

    return list(set(synonyms))


if __name__ == '__main__':
    data = pd.read_csv(
        "outputs/kg/foods/ncbi_taxonomy.csv",
        converters={
            'OtherNames': lambda x: literal_eval(x) if x != '' else None,
        },
    )

    data['synonyms'] = data['OtherNames'].apply(parse_other_names)

    # For the different taxonomy ids, the sysnonyms should be unique.
    def count_synonyms(row, ht):
        for synonym in row['synonyms']:
            ht[synonym.lower()] += [row['TaxId']]

    ht = defaultdict(list)
    data.apply(lambda row: count_synonyms(row, ht), axis=1)
    entities_ambiguous = []
    for synonym, tax_ids in ht.items():
        if len(tax_ids) > 1:
            data_ = data[data['TaxId'].isin(tax_ids)].copy()
            data_['synonyms'].apply(lambda x: x.remove(synonym))
            entities_ambiguous += [{
                'TaxId': data_['TaxId'].tolist(),
                'ScientificName': ' | '.join(data_['ScientificName'].tolist()),
                'synonyms': [synonym],
            }]
    data['TaxId'] = data['TaxId'].apply(lambda x: [x])
    data = pd.concat([data, pd.DataFrame(entities_ambiguous)], axis=0)
    data = data[['TaxId', 'ScientificName', 'Rank', 'synonyms']]
    data = data.rename(columns={
        'TaxId': 'ncbi_taxon_ids',
        'ScientificName': 'scientific_name',
        'Rank': 'rank',
    })
    print(data)
