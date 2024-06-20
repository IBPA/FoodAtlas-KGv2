import pandas as pd

from bs4 import BeautifulSoup


if __name__ == '__main__':
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

        cdno_rows += [{
            'id': cdno_id,
            'parents': parents,
            'fdc_nutrient_ids': fdc_nutrient_ids,
            'chebi_ids': chebi_ids,
        }]

    cdno = pd.DataFrame(cdno_rows).set_index('id')
    # cdno = label_is_concentration_of(cdno)

    # Note: CDNO and ChEBI is one-to-one.
    cdno = cdno[cdno['fdc_nutrient_ids'].apply(len) > 0]
    cdno['chebi_id'] = cdno['chebi_ids'].apply(lambda x: x[0] if x else None)

    # Disambiguate FDC IDs such that each FDC ID can only be associated to one ChEBI ID.
    fdc_ids = set()
    for fdc_nutrient_ids in cdno['fdc_nutrient_ids']:
        fdc_ids.update(fdc_nutrient_ids)

    cdno_cleaned_rows = []
    for fdc_id in fdc_ids:
        if fdc_id == '1071':
            continue

        cdno_fdc = cdno[cdno['fdc_nutrient_ids'].apply(lambda x: fdc_id in x)]
        if len(cdno_fdc) == 1:
            # Return if FDC ID is associated to only one ChEBI ID.
            cdno_cleaned_rows += [cdno_fdc.iloc[0]]
        else:
            # Drop rows with missing ChEBI IDs. If only one row remains, return it.
            # Otherwise, try to return the ChEBI ID that is the parent manually.
            cdno_cleaned_row = cdno_fdc.dropna(subset=['chebi_id'])
            if len(cdno_cleaned_row) == 1:
                cdno_cleaned_rows += [cdno_cleaned_row.iloc[0]]
            else:
                if fdc_id == '1162':
                    cdno_cleaned_rows += [cdno_cleaned_row.iloc[0]]
                elif fdc_id == '1038':
                    cdno_cleaned_rows += [cdno_cleaned_row.iloc[0]]
                elif fdc_id == '1167':
                    cdno_cleaned_rows += [cdno_cleaned_row.iloc[1]]
                elif fdc_id == '1183':
                    cdno_cleaned_rows += [cdno_cleaned_row.iloc[0]]
                else:
                    raise ValueError(fdc_id)
    cdno_cleaned = pd.DataFrame(cdno_cleaned_rows)

    # Check if FDC and ChEBI is one-to-one.
    map_fdc2chebi = {}
    for _, row in cdno_cleaned.iterrows():
        for fdc_id in row['fdc_nutrient_ids']:
            if fdc_id not in map_fdc2chebi:
                map_fdc2chebi[fdc_id] = set()

            if pd.notna(row['chebi_id']):
                map_fdc2chebi[fdc_id].add(row['chebi_id'])
            else:
                map_fdc2chebi[fdc_id].add(row.name)

    for fdc_id, chebi_ids in map_fdc2chebi.items():
        if len(chebi_ids) != 1:
            print(fdc_id, chebi_ids)
            raise ValueError(fdc_id)

    cdno_cleaned.to_csv(
        "outputs/data_processing/cdno_cleaned.tsv",
        sep='\t',
    )
