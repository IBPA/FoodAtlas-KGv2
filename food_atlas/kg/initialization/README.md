# Initialization for FoodAtlas Knowledge Graph

The aim for the data processing is to construct a knowledge graph consisted of `entities`, `relations`, `evidence`, and `lookups`.

## Entities

### Food

Each food entity has the following fields:
- A unique `foodatlas_id`
- A unique `common_name`
- A unique `scientific_name`
- Unique `synonyms`
- `external_ids`
- `food_processing`
- `food_part`
- `ambiguous`

The procedure of initializing food entities is as follows:
- Get NCBI Taxonomy IDs from FooDB and FDC.
- Retrieve scientific names as well as synonyms from NCBI Taxonomy.
- Iterate all the synonyms. If a sysnonym is not unique to a scientific name, have it as an individual food entity and remove the synonym from the corresponding scientific names.
    - For example, "malanga" is a synonym of both "Colocasia esculenta" and "Xanthosoma sagittifolium" according to NCBI Taxonomy. Therefore, "malanga" is an individual food entity with a ambiguous scientific name (i.e., "Colocasia esculenta | Xanthosoma sagittifolium"). This ensures that given the same string, it can only be mapped to one food entity.
    - "malanga" will be removed from the synonyms of "Colocasia esculenta" and "Xanthosoma sagittifolium".
    - "malanga" entity will be also annotated with the `ambiguous` field set to `True`.

## Lookup table

### Food

The food lookup table is a dictionary with the key being the `food_name` and the value being the `foodatlas_id`.

## Procedure

The following files were run sequentially:
1. `extract_ncbi_taxon_ids.py`
1. `retrieve_ncbi_taxon_food_names.py`
1. `initialize_food_entities.py`
1. `initialize_food_lookup_table.py`
