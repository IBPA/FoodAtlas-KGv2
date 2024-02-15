def get_lookup_key_by_id(id_type: str, id_value: str | int) -> str:
    LOOKUP_BY_ID = "_FAEXTID:{}_SEP_{}"

    if id_type == 'ncbi_taxon_id':
        return LOOKUP_BY_ID.format('ncbi_taxon_id', id_value)
    elif id_type == 'pubchem_cid':
        return LOOKUP_BY_ID.format('pubchem_cid', id_value)
    elif id_type == 'fdc_nutrient_ids':
        return LOOKUP_BY_ID.format('fdc_nutrient_ids', id_value)
    elif id_type == 'foodb_ids':
        return LOOKUP_BY_ID.format('foodb_ids', id_value)
    elif id_type == 'fdc_ids':
        return LOOKUP_BY_ID.format('fdc_ids', id_value)
    else:
        raise ValueError(f"Unknown ID type: {id_type}")
