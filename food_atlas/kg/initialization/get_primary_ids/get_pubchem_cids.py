import pandas as pd


def get_pubchem_cid_from_fdc():
    chemicals = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/nutrient.csv"
    )
    chemical_ids_used = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food_nutrient.csv",
        usecols=['nutrient_id'],
    )['nutrient_id'].unique().tolist()
    chemicals = chemicals.query(f"id in {chemical_ids_used}").set_index('id')

    attrs = pd.read_excel(
        "data/FDC/Phytochemical_Spreadsheet.xlsx",
        sheet_name='FDC-Compounds(nutr)',
    ).set_index('id')

    def _get_pubchem_cid(row):
        if row.name in attrs.index:
            pubchem_cids = attrs.loc[[row.name], 'pubchem_compound_id'].tolist()
            if len(pubchem_cids) == 1:
                return pubchem_cids[0]
            elif len(pubchem_cids) > 1:
                return pubchem_cids[0]

        return None

    chemicals['pubchem_cid_from_kay'] \
        = chemicals.apply(_get_pubchem_cid, axis=1).astype('Int64')
    chemicals['name'].to_csv(
        "outputs/kg/initialization/_chemical_names_for_PIES_fdc_1.txt",
        index=False,
        header=False,
    )
    chemicals['name_formatted'] = ''
    chemicals = chemicals[[
        'name', 'name_formatted', 'pubchem_cid_from_kay',
    ]]

    # Use the PubChem Identifier Exchange Service to get the PubChem CID from the name.
    pubchem_cids = pd.read_csv(
        "outputs/kg/initialization/_PIES_fdc_1.txt",
        header=None,
        sep='\t',
        names=['name', 'pubchem_cid'],
        dtype={'name': str, 'pubchem_cid': 'Int64'}
    ).set_index('name')

    def _get_pubchem_cid_from_query(row):
        if row['name'] in pubchem_cids.index:
            cids = pubchem_cids.loc[
                [row['name']], 'pubchem_cid'
            ].dropna().unique().tolist()
            if len(cids) == 1:
                return cids[0]
            elif len(cids) > 1:
                return cids
        return None

    chemicals['pubchem_cid_from_query'] = chemicals.apply(
        _get_pubchem_cid_from_query, axis=1
    )

    def _get_name_formatted(row):
        if row['pubchem_cid_from_query'] is not None:
            return 'SKIP'

        terms = row['name'].strip().split(', ')
        if len(terms) == 2:
            if len(terms[1]) <= 2 and ' ' not in terms[0]:
                # Element.
                return terms[0]
            elif terms[1] in ['alpha', 'beta', 'gamma', 'delta']:
                # Different forms of the compound with unique CID.
                return f"{terms[1]}-{terms[0]}"
            else:
                return "SKIP_MANUAL"
        else:
            if 'FA ' in row['name']:
                return "SKIP_LIPIDS"
            else:
                return "SKIP_MANUAL"

    chemicals['name_formatted'] = chemicals.apply(
        _get_name_formatted, axis=1
    )
    with open("outputs/kg/initialization/_chemical_names_for_PIES_fdc_2.txt", 'w') as f:
        f.write(
            '\n'.join(
                [
                    x for x in chemicals['name_formatted'].dropna().tolist()
                    if 'SKIP' not in x
                ]
            )
        )

    # Use the PubChem Identifier Exchange Service to get the PubChem CID from the name.
    pubchem_cids_for_not_found = pd.read_csv(
        "outputs/kg/initialization/_PIES_fdc_2.txt",
        header=None,
        sep='\t',
        names=['name', 'pubchem_cid'],
        dtype={'name': str, 'pubchem_cid': 'Int64'}
    ).set_index('name')

    chemicals = chemicals.reset_index().set_index('name_formatted')
    chemicals.loc[pubchem_cids_for_not_found.index, 'pubchem_cid_from_query'] \
        = pubchem_cids_for_not_found['pubchem_cid']
    chemicals = chemicals.reset_index().set_index('id')
    chemicals.to_csv('outputs/kg/initialization/_pubchem_cids_fdc.tsv', sep='\t')

    # Manually get PubChem CIDs for SKIP_MANUAL, and save the file as
    # `pubchem_cids_fdc_manual.tsv`.


if __name__ == '__main__':
    get_pubchem_cid_from_fdc()
