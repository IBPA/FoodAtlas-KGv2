from ast import literal_eval

import pandas as pd

from ... import KnowledgeGraph
from ...entities._food import _create_food_entities_from_ncbi_taxonomy
from ....tests import unit_test_kg


def load_fdc_foundation_food():

    def _check_fdc(data_fdc):
        fdc_cls = pd.read_csv("data/FDC/fdc_classification.tsv", sep='\t')

        ht = {}
        for col in fdc_cls.columns:
            for food in fdc_cls[col].dropna():
                ht[food.strip()] = 0

        for food in data_fdc['description']:
            if food not in ht:
                raise ValueError(
                    f"Food name {food} not found in the FDC classification."
                )
            else:
                ht[food] += 1

        for food, count in ht.items():
            if count != 1:
                raise ValueError(
                    f"Food name {food} found {count} times in the FDC classification."
                )

    PATH_DATA_DIR = "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26"

    foundation_food_ids \
        = pd.read_csv(f"{PATH_DATA_DIR}/foundation_food.csv")['fdc_id'].tolist()
    data_food = pd.read_csv(f"{PATH_DATA_DIR}/food.csv")
    data_food = data_food[data_food['fdc_id'].isin(foundation_food_ids)]
    data_attr = pd.read_csv(f"{PATH_DATA_DIR}/food_attribute.csv")
    data_attr = data_attr[data_attr['fdc_id'].isin(foundation_food_ids)]
    data_attr = data_attr.set_index('fdc_id')

    def extract_urls(row):
        result = {
            'ncbi_taxon_url': None,
        }
        if row['fdc_id'] in data_attr.index:
            attr = data_attr.loc[row['fdc_id']]
            if type(attr) == pd.Series:
                attr = pd.DataFrame(attr).T
            attr = attr.set_index('name')

            result['ncbi_taxon_url'] = attr.loc['NCBI Taxon', 'value'] \
                if 'NCBI Taxon' in attr.index else None

        return pd.Series(result)

    data_food = pd.concat(
        [data_food, data_food.apply(extract_urls, axis=1)],
        axis=1
    )

    # Parse NCBI Taxon ID from URL.
    def _parse_ncbi_taxon_url(url):
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

    data_food['ncbi_taxon_id'] = data_food['ncbi_taxon_url'].apply(
        _parse_ncbi_taxon_url
    )
    data_food['ncbi_taxon_id'] = data_food['ncbi_taxon_id'].astype('Int64')
    data_food['description'] = data_food['description'].str.strip()

    _check_fdc(data_food)

    return data_food


def init_food_entities_from_ncbi_taxonomy(kg):
    records = pd.read_csv(
        "outputs/kg/initialization/_ncbi_taxonomy.tsv",
        sep='\t',
        converters={
            'OtherNames': lambda x: literal_eval(x) if x != '' else None,
        },
    )

    _create_food_entities_from_ncbi_taxonomy(kg.entities, records)


if __name__ == '__main__':
    kg = KnowledgeGraph(
        path_kg="outputs/kg",
    )

    init_food_entities_from_ncbi_taxonomy(kg)
    kg._disambiguate_synonyms()
    kg.save("outputs/kg")

    unit_test_kg.test_all(kg)
