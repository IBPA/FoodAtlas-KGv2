ID_PREFIX_MAPPER = {
    'ncbi_taxon_id': 'NCBI_Taxonomy_ID',
    'pubchem_cid': 'PubChem_Compound_ID',
    'fdc_nutrient_ids': 'FDC_Nutrient_ID',
    'foodb_ids': 'FooDB_Food_ID',
    'fdc_ids': 'FDC_ID',
}


def get_lookup_key_by_id(id_type: str, id_value: str | int) -> str:
    LOOKUP_BY_ID = "{}:{}"

    if id_type not in ID_PREFIX_MAPPER:
        raise ValueError(f"Unknown ID type: {id_type}")

    return LOOKUP_BY_ID.format(ID_PREFIX_MAPPER[id_type], id_value)
