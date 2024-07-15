from ast import literal_eval

import pandas as pd
from bs4 import BeautifulSoup


def load_cdno():
    """TODO: Copied and pasted from data_processing since the original data removed
    some entries.

    """
    soup = BeautifulSoup(open("data/CDNO/cdno.owl"), "xml")

    cdno_rows = []
    for class_ in soup.find_all('owl:Class'):
        cdno_id = class_.attrs.get('rdf:about')
        if cdno_id is None:
            continue

        if class_.find('owl:deprecated'):
            continue

        # Extract parents.
        parents = class_.find_all('rdfs:subClassOf')
        parents = [
            p.attrs.get('rdf:resource') for p in parents
            if p.attrs.get('rdf:resource') is not None
        ]

        # Extract FDC Nutrient IDs.
        refs = class_.find_all('oboInOwl:hasDbXref')
        fdc_nutrient_ids = [
            ref.text.split('USDA_fdc_id:')[-1] for ref in refs
            if ref.text.startswith('USDA_fdc_id')
        ]

        # Extract ChEBI IDs.
        chebi_ids = []
        eqs = class_.find_all('owl:equivalentClass')
        assert len(eqs) <= 1
        eq = eqs[0] if eqs else None
        if eq:
            for desc in eq.find_all('rdf:Description'):
                if 'CHEBI_' in desc.attrs.get('rdf:about'):
                    chebi_ids += [desc.attrs.get('rdf:about')]

        # Extract label.
        label = class_.find('rdfs:label')
        label = label.text if label else None

        cdno_rows += [{
            'id': cdno_id,
            'label': label,
            'parents': parents,
            'fdc_nutrient_ids': fdc_nutrient_ids,
            'chebi_ids': chebi_ids,
        }]

    cdno = pd.DataFrame(cdno_rows).set_index('id')

    cdno['chebi_id'] = cdno['chebi_ids'].apply(lambda x: x[0] if x else None)

    # Manually correct ChEBI IDs.
    cdno['chebi_id'] = cdno['chebi_id'].replace({
        'http://purl.obolibrary.org/obo/CHEBI_80096':
            'http://purl.obolibrary.org/obo/CHEBI_166888'
    })

    return cdno


def generate_chemical_groups_cdno(kg):
    """
    """
    cdno = load_cdno()

    # 1. amino acid and protein
    # 2. carbohydrate (including fiber)
    # 3. lipid
    # 4. vitamin
    # 5. others (organic acid, secondary metabolite)
    id2parent = {
        'http://purl.obolibrary.org/obo/CDNO_0200464': ['amino acid'],
        'http://purl.obolibrary.org/obo/CDNO_0200040': ['protein'],
        'http://purl.obolibrary.org/obo/CDNO_0200005': ['carbohydrate'],
        'http://purl.obolibrary.org/obo/CDNO_0200035': ['dietary fiber'],
        'http://purl.obolibrary.org/obo/CDNO_0200068': ['lipid'],
        'http://purl.obolibrary.org/obo/CDNO_0200179': ['vitamin'],
        # minerals
        'http://purl.obolibrary.org/obo/CDNO_0200004': ['ash'],
        'http://purl.obolibrary.org/obo/CDNO_0200136': ['mineral nutrient'],
        # others
        'http://purl.obolibrary.org/obo/CDNO_0200422': ['organic acid'],
        'http://purl.obolibrary.org/obo/CDNO_0200215': ['plant secondary metabolite'],
        'http://purl.obolibrary.org/obo/CDNO_0200002': ['water'],
    }

    def map_to_groups():
        def dfs(cdno_id):
            if cdno_id in id2parent:
                return id2parent[cdno_id]
            if cdno_id not in cdno.index:
                return []

            results = set()
            for parent_id in cdno.loc[cdno_id]['parents']:
                results.update(dfs(parent_id))
            id2parent[cdno_id] = sorted(list(results))

            return results

        cdno.apply(lambda row: dfs(row.name), axis=1)

    map_to_groups()
    cdno['cdno_groups'] = cdno.index.map(lambda x: id2parent[x])
    cdno = cdno[cdno['cdno_groups'].apply(len) > 0].copy()
    cdno['chebi'] = cdno['chebi_id'].apply(
        lambda x: int(x.split('_')[-1]) if x else None
    ).astype('Int64')
    cdno['cdno'] = cdno.index.map(lambda x: x.split('/')[-1])
    cdno = cdno[['chebi', 'cdno', 'cdno_groups']]

    chemicals = kg.entities._entities.query("entity_type == 'chemical'").copy()
    # chemicals = pd.read_csv(
    #     "outputs/kg/entities.tsv",
    #     sep='\t',
    #     converters={'external_ids': literal_eval},
    # ).query('entity_type == "chemical"').set_index('foodatlas_id')

    # ChEBI to CDNO.
    chebi2group = {}
    for _, row in cdno.iterrows():
        if not pd.isna(row['chebi']):
            chebi2group[row['chebi']] = row['cdno_groups']
    eid2group = {}
    for _, row in chemicals.iterrows():
        if 'chebi' in row['external_ids']:
            chebi = row['external_ids']['chebi'][0]
            if chebi in chebi2group:
                eid2group[row.name] = chebi2group[chebi]

    # # Mention counts.
    # triplets = pd.read_csv(
    #     "outputs/kg/triplets.tsv",
    #     sep='\t',
    #     converters={'metadata_ids': literal_eval},
    # ).query("relationship_id == 'r1'")
    # chemicals['count'] = 0
    # for _, row in triplets.iterrows():
    #     chemicals.at[row['tail_id'], 'count'] += len(row['metadata_ids'])
    # print(chemicals['count'].sum())

    def assign_group(row):
        if row.name in eid2group:
            groups = []
            for group in eid2group[row.name]:
                if group in ['amino acid', 'protein']:
                    groups.append('amino acid and protein')
                elif group in ['carbohydrate', 'dietary fiber']:
                    groups.append('carbohydrate (including fiber)')
                elif group in ['lipid']:
                    groups.append('lipid')
                elif group in ['vitamin']:
                    groups.append('vitamin')
                elif group in ['mineral nutrient', 'ash']:
                    groups.append('mineral (including derivatives)')
                else:
                    groups.append('others')

            return sorted(set(groups))

        return ['others']

    chemicals['cdno_groups'] = chemicals.apply(assign_group, axis=1)

    return chemicals['cdno_groups']
