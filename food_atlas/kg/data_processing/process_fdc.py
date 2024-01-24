import pandas as pd


if __name__ == '__main__':
    PATH_DATA_DIR = "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26"

    data = pd.read_csv(f"{PATH_DATA_DIR}/food.csv")
    ids_ff = pd.read_csv(f"{PATH_DATA_DIR}/foundation_food.csv")['fdc_id']
    data = data[data['fdc_id'].isin(ids_ff)]
    attrs_ff = pd.read_csv(f"{PATH_DATA_DIR}/food_attribute.csv").set_index('fdc_id')

    def extract_external_ids(row):
        result = {
            'foodon_url': None,
            'ncbi_taxon_url': None,
        }
        if row['fdc_id'] in attrs_ff.index:
            attr = attrs_ff.loc[row['fdc_id']]
            if type(attr) == pd.Series:
                attr = pd.DataFrame(attr).T
            attr = attr.set_index('name')

            for attr_name, col_name in zip(attr_names, col_names):
                result[col_name] = attr.loc[attr_name, 'value'] \
                    if attr_name in attr.index else None

        return pd.Series(result)

    attr_names = ['FoodOn Ontology ID for FDC item', 'NCBI Taxon']
    col_names = ['foodon_url', 'ncbi_taxon_url']
    data = pd.concat(
        [data, data.apply(extract_external_ids, axis=1)],
        axis=1
    )

    # Parse NCBI Taxon ID from URL.
    def parse_ncbi_taxon_url(url):
        if url is None:
            return None
        else:
            id_ = url.split('/')[-1]
            if id_.isdigit():
                return int(id_)
            elif id_.startswith('NCBITaxon_'):
                return int(id_.split('_')[-1])
            else:
                return int(id_.split('=')[-1])

    data['ncbi_taxon_id'] = data['ncbi_taxon_url'].apply(parse_ncbi_taxon_url)
    data = data.rename(columns={
        'fdc_id': 'fdc_id',
        'description': 'food_name',
    })
    data = data.drop(columns=[
        'food_category_id',
        'data_type',
        'ncbi_taxon_url',
        'publication_date',
    ])

    def append_external_ids(row):
        external_ids = []
        external_ids += [{'FDC': [row['fdc_id']]}]
        if pd.notna(row['ncbi_taxon_id']):
            external_ids += [{'NCBITaxon': [int(row['ncbi_taxon_id'])]}]
        if row['foodon_url']:
            external_ids += [{'FoodOn': [row['foodon_url']]}]

        return external_ids

    data['external_ids'] = data.apply(append_external_ids, axis=1)
    data = data.drop(columns=['fdc_id', 'ncbi_taxon_id', 'foodon_url'])
    print(data)
