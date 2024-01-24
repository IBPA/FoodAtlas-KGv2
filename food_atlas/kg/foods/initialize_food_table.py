import os

import pandas as pd
# from Bio import Entrez

# Entrez.email = "fzli@ucdavis.edu"
# Entrez.api_key = "7631dcfb0fa03378db8adae415c8fee60309"


def import_from_fdc():
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
    print(data)

    def append_table():
        pass

    def append_entities():
        pass

    return data

    # data.to_csv("outputs/kg/foods/fdc.csv")

def import_from_foodb():
    pass


def import_from_ncbi_taxonomy():
    pass


if __name__ == '__main__':
    if os.path.exists("outputs/kg/foods/table.tsv"):
        raise Exception("The table already exists. Please delete it first.")
    if os.path.exists("outputs/kg/entities.tsv"):
        raise Exception("The table already exists. Please delete it first.")

    table_rows = []
    entities_rows = []

    import_from_fdc(table_rows, entities_rows)
    # data_fdc = pd.read_csv("outputs/kg/foods/fdc.csv")
    # data_foodb = pd.read_csv("outputs/kg/foods/foodb.csv")
    # data = pd.concat([data_fdc, data_foodb], axis=0)
    # data = data.drop(columns=['Unnamed: 0'])
    # data = data[[
    #     'orig_name',
    #     'scientific_name',
    #     'ncbi_taxon_id',
    #     'itis_id',
    #     'fdc_id',
    #     'foodb_id',
    #     'food_group',
    #     'food_subgroup',
    # ]]
    # data = data.astype({
    #     'ncbi_taxon_id': 'Int64'
    # })
    # def parse_other_names(other_names):
    #     print(other_names)

    # data['synonyms'] = data['OtherNames'].apply(parse_other_names)
