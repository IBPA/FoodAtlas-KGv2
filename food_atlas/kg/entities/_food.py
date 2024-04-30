import logging
from collections import OrderedDict

import pandas as pd
from inflection import singularize, pluralize
from tqdm import tqdm

from ..utils import constants, merge_sets
from .._query import query_ncbi_taxonomy

logger = logging.getLogger(__name__)


def _group_synonyms(synonyms_groups: list[list[str]]):
    """A list of synonym groups, each representing singluar and plural forms of a word.
    This function groups the synonyms no group is a subset is another group, and
    overlaps are merged.

    For example:
    ```
    INPUT = [
        ['apple', 'apples'],
        ['apples', 'apple'],
        ['person', 'people', 'peoples'],
        ['people', 'peoples'],
        ['olive', 'olives'],
        ['olives', 'olife'],
    ]

    OUTPUT = [
        ['apple', 'apples'],
        ['person', 'people', 'peoples'],
        ['olive', 'olives', 'olife'],
    ]
    ```

    """
    logger.info("Start grouping synonyms...")

    synonyms_groups = [set(synonyms) for synonyms in synonyms_groups]
    synonyms_groups = merge_sets(synonyms_groups)
    synonyms_groups = [sorted(synonyms) for synonyms in synonyms_groups]

    logger.info("Completed grouping synonyms!")

    return synonyms_groups


def _create_food_entities_from_ncbi_taxonomy(
    entities,
    records: pd.DataFrame,
):
    """Helper for creating food entities, which have NCBI Taxonomy IDs.

    Args:
        entities (Entities): Entities object.
        records (pd.DataFrame): Records from NCBI Taxonomy.

    """
    logger.info("Start creating entities with NCBI Taxonomy IDs...")

    def _parse_names(row):
        scientific_name = row['ScientificName']
        other_names = row['OtherNames']

        synonyms_scientific = [scientific_name]
        synonyms_common = []
        synonyms_others = []
        if row['OtherNames'] is not None:
            synonyms_scientific += other_names['Synonym']
            synonyms_scientific += other_names['EquivalentName']
            if other_names['Name']:
                for name in other_names['Name']:
                    if name['ClassCDE'] in ['misspelling', 'authority']:
                        synonyms_scientific += [name['DispName']]

            synonyms_common += other_names['CommonName']
            if 'GenbankCommonName' in other_names:
                synonyms_common += [other_names['GenbankCommonName']]
            if 'BlastName' in other_names:
                synonyms_common += [other_names['BlastName']]

            synonyms_others += other_names['Includes']

        row['scientific_name'] = scientific_name.strip().lower()

        if synonyms_common:
            synonyms_common_sp = []
            for name in synonyms_common:
                synonyms_common_sp += [singularize(name), pluralize(name)]
            synonyms_common += synonyms_common_sp
            row['common_name'] = min(synonyms_common, key=len).strip().lower()
        else:
            row['common_name'] = row['scientific_name']

        # Include abbreviations for scientific names.
        if len(scientific_name.split(' ')) > 1:
            # Consider only species or lower.
            synonyms_scientific_abbr = []
            for name in synonyms_scientific:
                terms = name.split(' ')
                terms[0] = terms[0][0] + '.'
                synonyms_scientific_abbr += [' '.join(terms)]
            synonyms_scientific += synonyms_scientific_abbr

        synonyms = synonyms_common + synonyms_others + synonyms_scientific
        synonyms = list(OrderedDict.fromkeys(synonyms).keys())
        synonyms = [x.strip().lower() for x in synonyms]
        row['synonyms'] = synonyms + [constants.get_lookup_key_by_id(
            'ncbi_taxon_id', row['TaxId']
        )]

        return row

    entities_new = records.copy()
    entities_new[entities.COLUMNS] = None
    entities_new = entities_new.apply(_parse_names, axis=1)
    entities_new = entities_new.rename(columns={'TaxId': 'ncbi_taxon_id'})
    entities_new['external_ids'] \
        = entities_new['ncbi_taxon_id'].apply(
            lambda x: {'ncbi_taxon_id': x}
        )

    entities_new['foodatlas_id'] = [
        f"e{i}"
        for i in range(entities._curr_eid, entities._curr_eid + len(entities_new))
    ]
    entities._curr_eid += len(entities_new)

    entities_new['entity_type'] = 'food'
    entities_new = entities_new[entities.COLUMNS].set_index('foodatlas_id')
    entities._entities = pd.concat([entities._entities, entities_new])
    entities._update_lut(entities_new)

    logger.info("Completed!")


def _create_food_entities_from_synonym_groups(
    entities,
    synonym_groups: list[list[str]],
):
    """Helper for creating food entities, which do not have NCBI Taxonomy IDs.

    Args:
        entities (Entities): Entities object.
        synonym_groups (list[list[str]]): Groups of synonyms.

    """
    logger.info("Start creating entities without NCBI Taxonomy IDs...")

    entities_new_rows = []
    for synonyms in synonym_groups:
        found = False
        for name in synonyms:
            if entities.get_entity_ids('food', name):
                found = True
                break

        if not found:
            entities_new_rows += [{
                'foodatlas_id': f"e{entities._curr_eid}",
                'entity_type': 'food',
                'common_name': min(synonyms, key=len),
                'scientific_name': '',
                'synonyms': synonyms,
                'external_ids': {},
            }]
            entities._curr_eid += 1

    entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
    entities._entities = pd.concat([entities._entities, entities_new])
    entities._update_lut(entities_new)

    logger.info("Completed!")

def create_food_entities(
    entities,
    entity_names_new: list[str],
):
    """Helper for creating food entities. For food entity, we first query
    NCBI Taxonomy to see if there is an ID. If not, we create food entities without
    NCBI Taxonomy IDs.

    Args:
        entities (Entities): Entities object.
        entity_names_new (list[str]): Names of the entities.

    """
    # Step 0. Create synonyms by applying singularize and pluralize.
    entity_synonyms = [
        list(set([name, singularize(name), pluralize(name)]))
        for name in entity_names_new
    ]
    entity_names_all \
        = list(set([name for names in entity_synonyms for name in names]))

    # Step 1. Query NCBI Taxonomy to see if there is an ID.
    records_ncbi_taxonomy = query_ncbi_taxonomy(
        entity_names_all,
        entities.path_kg,
        entities.path_cache_dir,
    )
    _create_food_entities_from_ncbi_taxonomy(entities, records_ncbi_taxonomy)

    # Step 2. Create entities that are still new to the KG.
    entity_synonyms_grouped = _group_synonyms(entity_synonyms)
    _create_food_entities_from_synonym_groups(entities, entity_synonyms_grouped)

    # Step 3. Link all names to entities and update synonyms & lookup table.
    for synonyms in entity_synonyms_grouped:
        found = False
        for name in synonyms:
            eids = entities.get_entity_ids('food', name)
            for eid in eids:
                entities._update_entity_synonyms(eid, synonyms)
                found = True
        if not found:
            raise ValueError(f"Entity not found for synonyms: {synonyms}")
