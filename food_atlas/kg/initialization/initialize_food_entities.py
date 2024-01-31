from ast import literal_eval

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
        "outputs/kg/initialization/ncbi_taxonomy.tsv",
        sep='\t',
        converters={
            'OtherNames': lambda x: literal_eval(x) if x != '' else None,
        },
    )

    data['synonyms'] = data['OtherNames'].apply(parse_other_names)

    # For the different taxonomy ids, the sysnonyms should be unique.
    def count_synonyms(row, ht):
        for synonym in row['synonyms']:
            ht[synonym.lower()] += [row['TaxId']]

    # # Resolve ambiguous synonyms. For example, "malanga" is a synonym for
    # # Colocasia esculenta and Xanthosoma sagittifolium
    # data['ambiguous'] = False
    # ht = defaultdict(list)
    # data.apply(lambda row: count_synonyms(row, ht), axis=1)
    # entities_ambiguous = []
    # for synonym, tax_ids in ht.items():
    #     if len(tax_ids) > 1:
    #         data_ = data[data['TaxId'].isin(tax_ids)].copy()
    #         data_['synonyms'].apply(lambda x: x.remove(synonym))
    #         entities_ambiguous += [{
    #             'TaxId': data_['TaxId'].tolist(),
    #             'ScientificName': ' | '.join(data_['ScientificName'].tolist()),
    #             'synonyms': [synonym],
    #             'ambiguous': True,
    #         }]
    # data['TaxId'] = data['TaxId'].apply(lambda x: [x])
    # data = pd.concat([data, pd.DataFrame(entities_ambiguous)], axis=0)
    data = data.rename(columns={
        'TaxId': 'ncbi_taxon_id',
        'ScientificName': 'scientific_name',
    })
    # data['synonyms'] = data.apply(
    #     lambda row: list(set(row['synonyms'] + [row['scientific_name']]))
    #     if not row['ambiguous'] else row['synonyms'],
    #     axis=1,
    # )
    data['synonyms'] = data.apply(
        lambda row: list(set(row['synonyms'] + [row['scientific_name']])),
        axis=1,
    )
    # Default common name is the shortest synonym. If there are no synonyms, the
    # common name is the scientific name.
    data['common_name'] = data.apply(
        lambda row: min(row['synonyms'], key=len),
        axis=1,
    )
    data['external_ids'] \
        = data['ncbi_taxon_id'].apply(lambda x: {'ncbi_taxon_id': x})
    # data['food_part'] = 'UNCATEGORIZED'
    # data['food_processing'] = 'UNCATEGORIZED'
    # data['food_category'] = 'UNCATEGORIZED'
    data['foodatlas_id'] = [f"e{i}" for i in range(1, len(data) + 1)]
    data['entity_type'] = 'food'
    data = data[[
        'foodatlas_id',
        'entity_type'
        'common_name',
        'scientific_name',
        'synonyms',
        'external_ids',
        # 'food_part',
        # 'food_processing',
        # 'food_category',
    ]]

    data.to_csv("outputs/kg/entities.tsv", sep='\t', index=False)
