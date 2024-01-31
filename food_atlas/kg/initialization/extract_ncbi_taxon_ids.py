import pandas as pd
from Bio import Entrez

Entrez.email = "fzli@ucdavis.edu"
Entrez.api_key = "7631dcfb0fa03378db8adae415c8fee60309"


def get_ncbi_taxon_ids_from_fdc():
    PATH_DATA_DIR = "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26"

    data = pd.read_csv(f"{PATH_DATA_DIR}/food.csv")
    ids_ff = pd.read_csv(f"{PATH_DATA_DIR}/foundation_food.csv")['fdc_id']
    data = data[data['fdc_id'].isin(ids_ff)]
    attrs_ff = pd.read_csv(f"{PATH_DATA_DIR}/food_attribute.csv").set_index('fdc_id')

    def extract_external_ids(row):
        result = {
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

    attr_names = ['NCBI Taxon']
    col_names = ['ncbi_taxon_url']
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

    return data['ncbi_taxon_id'].dropna().astype(int).tolist()


def get_ncbi_taxon_ids_from_foodb():
    data = pd.read_json("data/FooDB/Food.json", lines=True)
    ncbi_taxon_ids = data['ncbi_taxonomy_id'].dropna().astype(int).tolist()

    # Also extract from scientific names.
    scientific_names = data.query(
        "ncbi_taxonomy_id.isna() & name_scientific.notna() & name_scientific != ''"
    )['name_scientific'].str.lower().tolist()

    for scientific_name in scientific_names:
        # Reformat.
        scientific_name \
            = scientific_name.replace('× ', 'x ').replace(' ssp. ', ' subsp. ')
        # Remove authority.
        scientific_name \
            = scientific_name.replace(' l.', '').replace(' f. alba dc.', '')
        handle = Entrez.esearch(db='taxonomy', term=scientific_name)
        record = Entrez.read(handle)
        if len(record['IdList']) == 1:
            ncbi_taxon_ids.append(int(record['IdList'][0]))

    return ncbi_taxon_ids


if __name__ == '__main__':
    ncbi_taxon_ids_fdc = get_ncbi_taxon_ids_from_fdc()
    ncbi_taxon_ids_foodb = get_ncbi_taxon_ids_from_foodb()

    with open('outputs/kg/initialization/ncbi_taxon_ids.txt', 'w') as f:
        for id_ in list(set(ncbi_taxon_ids_fdc + ncbi_taxon_ids_foodb)):
            f.write(f"{id_}\n")
