import pandas as pd

from ..utils import load_food_lookup_table, load_food_entities


def _check_fdc(data_fdc):
    fdc_cls = pd.read_csv("data/FDC/fdc_classification.tsv", sep='\t')

    ht = {}
    for col in fdc_cls.columns:
        for food in fdc_cls[col].dropna():
            ht[food.strip()] = 0

    for food in data_fdc['description']:
        if food not in ht:
            raise ValueError(f"Food name {food} not found in the FDC classification.")
        else:
            ht[food] += 1

    for food, count in ht.items():
        if count != 1:
            raise ValueError(
                f"Food name {food} found {count} times in the FDC classification."
            )

def _load_foundation_food():
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

    data_food = pd.concat([data_food, data_food.apply(extract_urls, axis=1)], axis=1)

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

    data_food['ncbi_taxon_id'] = data_food['ncbi_taxon_url'].apply(parse_ncbi_taxon_url)
    data_food['ncbi_taxon_id'] = data_food['ncbi_taxon_id'].astype('Int64')
    data_food['description'] = data_food['description'].str.strip()

    _check_fdc(data_food)

    return data_food


if __name__ == '__main__':
    lut = load_food_lookup_table()
    entities = load_food_entities()
    data_fdc = _load_foundation_food()
    fdc_cls = pd.read_csv("data/FDC/fdc_classification.tsv", sep='\t')

    entities = entities.set_index('foodatlas_id')
    entities['ncbi_taxon_id'] = entities['external_ids'].apply(
        lambda x: x['ncbi_taxon_id'] if 'ncbi_taxon_id' in x else None
    )
    entities['ncbi_taxon_id'] = entities['ncbi_taxon_id'].astype('Int64')

    # Merge FooDB non-many foods to FoodAtlas IDs using NCBI Taxonomy IDs.
    foods_many = fdc_cls['Processed many'].tolist()
    data_fdc_non_many = data_fdc.query(
        "ncbi_taxon_id.notna() "
        f"& (~description.isin({foods_many}))"
    )

    def merge_fdc_by_ncbi_taxon_id(row):
        if pd.isna(row['ncbi_taxon_id']):
            return

        ncbi_taxon_id = row['ncbi_taxon_id']
        fdc_foods = data_fdc_non_many.query(
            f"ncbi_taxon_id == {ncbi_taxon_id}"
        )
        if len(fdc_foods) != 0:
            fdc_ids = fdc_foods['fdc_id'].tolist()
            fdc_names = fdc_foods['description'].str.lower().tolist()
            row['external_ids']['fdc_ids'] = fdc_ids
            for name in fdc_names:
                if name in lut and row.name not in lut[name]:
                    lut[name] += [row.name]
                    row['synonyms'] += [name]
                elif name not in lut:
                    lut[name] = [row.name]
                    row['synonyms'] += [name]

    entities.apply(merge_fdc_by_ncbi_taxon_id, axis=1)

    # Merge the rest of the foods by creating new nodes.
    data_fdc_rest = data_fdc.query(
        f"ncbi_taxon_id.isna() | description.isin({foods_many})"
    )

    # Sanity check: No food should be found in the lut.
    found_in_lut = []
    data_fdc_rest['description'].apply(
        lambda x: found_in_lut.append(x) if x in lut else None
    )
    assert len(found_in_lut) == 0

    # Create new entities for the rest of the foods.
    def _merge_entity(row):
        global foodatlas_id_curr
        global entities_new_rows
        global lut

        entities_new_rows += [{
            'foodatlas_id': f"e{foodatlas_id_curr}",
            'common_name': row['description'].lower(),
            'scientific_name': '',
            'synonyms': [row['description'].lower()],
            'external_ids': {'fdc_id': [row['fdc_id']]},
        }]
        lut[row['description'].lower()] = [f"e{foodatlas_id_curr}"]
        foodatlas_id_curr += 1

    entities = entities.reset_index()
    foodatlas_id_curr = entities['foodatlas_id'].str.slice(1).astype(int).max() + 1
    entities_new_rows = []
    data_fdc_rest.apply(lambda row: _merge_entity(row), axis=1)

    entities_new = pd.DataFrame(entities_new_rows)
    entities = pd.concat([entities, entities_new], axis=0)
    entities = entities.drop(columns=['ncbi_taxon_id'])

    # Save the results.
    entities.to_csv("outputs/kg/food_entities.tsv", sep='\t', index=False)
    pd.DataFrame(lut.items(), columns=['name', 'foodatlas_id']).to_csv(
        "outputs/kg/food_lookup_table.tsv", sep='\t', index=False
    )
