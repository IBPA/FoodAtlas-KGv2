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

    # if id_type == 'ncbi_taxon_id':
    #     return LOOKUP_BY_ID.format('NCBI_Taxonomy_ID', id_value)
    # elif id_type == 'pubchem_cid':
    #     return LOOKUP_BY_ID.format('PubChem_Compound_ID', id_value)
    # elif id_type == 'fdc_nutrient_ids':
    #     return LOOKUP_BY_ID.format('FDC_Nutrient_ID', id_value)
    # elif id_type == 'foodb_ids':
    #     return LOOKUP_BY_ID.format('FooDB_Food_ID', id_value)
    # elif id_type == 'fdc_ids':
    #     return LOOKUP_BY_ID.format('FDC_ID', id_value)
    # else:
    #     raise ValueError(f"Unknown ID type: {id_type}")
