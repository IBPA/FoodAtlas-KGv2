import logging

import pandas as pd
from tqdm import tqdm

from ... import KnowledgeGraph
from ._load_chebi import (
    load_mapper_name_to_chebi_id,
    load_mapper_chebi_id_to_names,
)
from ._load_cdno import load_cdno
from ._load_fdc import load_fdc_nutrient
from ._load_pubchem import (
    load_mapper_chebi_id_to_pubchem_cid,
    load_mapper_pubchem_cid_to_mesh_id,
)
from ._load_mesh import load_mesh

logger = logging.getLogger(__name__)
tqdm.pandas()


def _add_to_lut(row, lut_chemical):
    if row['entity_type'] != 'chemical':
        return
    for syn in row['synonyms']:
        if syn in lut_chemical:
            raise ValueError(f"Duplicate synonym: {syn}")
        lut_chemical[syn] = [row.name]


def _append_entities_from_chebi(kg) -> KnowledgeGraph:
    """Prepare chemical entities from ChEBI. Each entity should have unique set of
    synonyms. Original ChEBI chemical entities with ambiguous synonyms were isolated
    and stored as placeholder entities.

    Returns:
        KnowledgeGraph: KG with updated chemical entities.

    """
    logger.info("Initializing chemical entities from ChEBI.")

    # Unique entities from ChEBI.
    chebi2name = load_mapper_chebi_id_to_names()

    # Placeholder entities with ambiguous names.
    name2chebi = load_mapper_name_to_chebi_id()
    name2chebi_ph = name2chebi[name2chebi['CHEBI_ID'].apply(len) > 1].copy()
    name2chebi_ph['CHEBI_ID'] = name2chebi_ph['CHEBI_ID'].apply(lambda x: sorted(x))
    name2chebi_ph['_chebi_id_str'] = name2chebi_ph['CHEBI_ID'].apply(str)
    phs = name2chebi_ph.groupby('_chebi_id_str').progress_apply(
        lambda x: pd.Series({
            'CHEBI_ID': x['CHEBI_ID'].values[0],
            'NAME': x['NAME'].tolist(),
        })
    ).reset_index(drop=True)

    # Remove placeholder entity names from unique entity synonyms.
    chebi2name = chebi2name.set_index('CHEBI_ID')
    for _, row in phs.iterrows():
        ids = row['CHEBI_ID']
        for id_ in ids:
            for name in row['NAME']:
                chebi2name.at[id_, 'NAME'].remove(name)
    chebi2name = chebi2name.reset_index()

    # Add unique ChEBI entities.
    entities_new_rows = []
    for _, row in chebi2name.iterrows():
        entities_new_rows.append({
            'foodatlas_id': f"e{kg.entities._curr_eid}",
            'entity_type': 'chemical',
            'common_name': row['NAME'][0],
            'scientific_name': None,  # TODO: map to IUPAC name.
            'synonyms': row['NAME'],
            'external_ids': {
                'chebi': [row['CHEBI_ID']],
            },
        })
        kg.entities._curr_eid += 1
    entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
    kg.entities._entities = pd.concat([kg.entities._entities, entities_new])
    kg.entities._lut_chemical = {}
    entities_new.progress_apply(
        lambda row: _add_to_lut(row, kg.entities._lut_chemical),
        axis=1,
    )

    logger.info(f"Added {len(entities_new)} unique chemical entities from ChEBI.")

    # Add placeholder entities.
    chebi2fa = {}
    for _, row in entities_new.iterrows():
        chebi_id = row['external_ids']['chebi'][0]
        chebi2fa[chebi_id] = row.name

    entities_new_rows = []
    for _, row in phs.iterrows():
        entities_new_rows.append({
            'foodatlas_id': f"e{kg.entities._curr_eid}",
            'entity_type': 'chemical',
            'common_name': row['NAME'][0],
            'scientific_name': None,
            'synonyms': row['NAME'],
            'external_ids': {
                '_placeholder_to': [chebi2fa[id_] for id_ in row['CHEBI_ID']],
            },
        })
        kg.entities._curr_eid += 1
    entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
    kg.entities._entities = pd.concat([kg.entities._entities, entities_new])
    entities_new.progress_apply(
        lambda row: _add_to_lut(row, kg.entities._lut_chemical),
        axis=1,
    )
    logger.info(f"Added {len(entities_new)} placeholder chemical entities.")

    return kg


def _append_entities_from_cdno(kg) -> KnowledgeGraph:
    """Prepare chemical entities from CDNO. For ones that are associated with existing
    entities imported from ChEBI, link them. For the rest, create new entities. Each
    new entity will have a unique CDNO ID as primary ID. CDNO also links to FDC Nutrient
    IDs. CDNO and ChEBI is 1-to-1 mapping.

    Returns:
        KnowledgeGraph: KG with updated chemical entities.

    """
    logger.info("Initializing chemical entities from CDNO.")

    cdno = load_cdno()

    # Collect ChEBI IDs in the KG.
    chebi2fa = {}
    for _, row in kg.entities._entities.iterrows():
        if row['entity_type'] == 'chemical' and 'chebi' in row['external_ids']:
            chebi_id = row['external_ids']['chebi'][0]
            chebi2fa[chebi_id] = row.name

    # Add CDNO to existing entities while recording new entities.
    n_linked = 0
    entities_not_added = []
    for _, row in cdno.iterrows():
        chebi_id = row['chebi_id']
        if chebi_id in chebi2fa:
            fa_id = chebi2fa[chebi_id]
            kg.entities._entities.at[fa_id, 'external_ids']['cdno'] = [row['cdno_id']]
            if row['fdc_nutrient_ids']:
                kg.entities._entities.at[fa_id, 'external_ids']['fdc_nutrient'] \
                    = row['fdc_nutrient_ids']
            n_linked += 1
        else:
            entities_not_added.append(row)

    logger.info(f"Linked {n_linked} CDNO IDs to the entities in the KG.")

    # Add new CDNO entities.
    entities_not_added = pd.DataFrame(entities_not_added)
    entities_new_rows = []
    for _, row in entities_not_added.iterrows():
        external_ids = {}
        if pd.notna(row['chebi_id']):
            external_ids['chebi'] = [row['chebi_id']]
        external_ids['cdno'] = [row['cdno_id']]
        external_ids['fdc_nutrient'] = row['fdc_nutrient_ids']

        entities_new_rows += [{
            'foodatlas_id': f"e{kg.entities._curr_eid}",
            'entity_type': 'chemical',
            'common_name': row['label'],
            'scientific_name': None,
            'synonyms': [row['label']],
            'external_ids': external_ids,
        }]
        kg.entities._curr_eid += 1
    entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')

    kg.entities._entities = pd.concat([kg.entities._entities, entities_new])
    entities_new.progress_apply(
        lambda row: _add_to_lut(row, kg.entities._lut_chemical),
        axis=1,
    )

    logger.info(f"Added {len(entities_new)} unique chemical entities from CDNO.")

    return kg


def _append_entities_from_fdc(kg) -> KnowledgeGraph:
    """Prepare chemical entities from FDC Nutrients. This is done because CDNO only
    links to half of FDC Nutrient IDs covered by Foundation food. Each of the newly
    generated entities will have a unique FDC Nutrient ID as primary ID. FDC Nutrient

    Returns:
        KnowledgeGraph: KG with updated chemical entities.

    """
    logger.info("Initializing chemical entities from FDC.")

    # Collect FDC NIDs in FoodAtlas.
    fdc2fa = {}
    def get_fdc2fa(row):
        if row['entity_type'] != 'chemical':
            return
        if 'fdc_nutrient' in row['external_ids']:
            fdc_ids = row['external_ids']['fdc_nutrient']
            for fdc_id in fdc_ids:
                if fdc_id in fdc2fa:
                    raise ValueError(f"Duplicate FDC ID: {fdc_id}")
                fdc2fa[fdc_id] = row.name
    kg.entities._entities.progress_apply(get_fdc2fa, axis=1)

    # Link FDC to existing entities while recording new entities.
    fdc = load_fdc_nutrient()
    n_linked = 0
    entities_not_added = []
    for _, row in fdc.iterrows():
        if row.name in fdc2fa:
            n_linked += 1
        elif row['name'] in kg.entities._lut_chemical:
            fa_id = kg.entities._lut_chemical[row['name']][0]
            kg.entities._entities.at[fa_id, 'external_ids']['fdc_nutrient'] \
                = [row.name]
            n_linked += 1
        else:
            entities_not_added.append(row)

    logger.info(f"Linked {n_linked} FDC Nutrient IDs to the entities in the KG.")

    # Add new FDC entities.
    entities_not_added = pd.DataFrame(entities_not_added)
    entities_new_rows = []
    for _, row in entities_not_added.iterrows():
        entities_new_rows += [{
            'foodatlas_id': f"e{kg.entities._curr_eid}",
            'entity_type': 'chemical',
            'common_name': row['name'],
            'scientific_name': None,
            'synonyms': [row['name']],
            'external_ids': {
                'fdc_nutrient': [row.name],
            },
        }]
        kg.entities._curr_eid += 1
    entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
    kg.entities._entities = pd.concat([kg.entities._entities, entities_new])
    entities_new.progress_apply(
        lambda row: _add_to_lut(row, kg.entities._lut_chemical),
        axis=1,
    )

    logger.info(f"Added {len(entities_new)} unique chemical entities from FDC.")

    return kg


if __name__ == '__main__':
    kg = KnowledgeGraph()

    # We initialize chemical entities by using ChEBI, CDNO, and FDC Nutrients. The
    # primary ID is ChEBI, followed by CDNO and FDC.
    kg = _append_entities_from_chebi(kg)
    kg = _append_entities_from_cdno(kg)
    kg = _append_entities_from_fdc(kg)

    # Map ChEBI to PubChem CID.
    n_mapped = 0
    chebi2cid = load_mapper_chebi_id_to_pubchem_cid()
    def map_chebi_id_to_pubchem_cid(row):
        global n_mapped

        if row['entity_type'] != 'chemical':
            return
        if 'chebi' not in row['external_ids']:
            return
        chebi_id = row['external_ids']['chebi'][0]
        if chebi_id in chebi2cid.index:
            row['external_ids']['pubchem_compound'] = [chebi2cid.loc[chebi_id]]
            n_mapped += 1
    kg.entities._entities.progress_apply(map_chebi_id_to_pubchem_cid, axis=1)

    logger.info(f"Mapped {n_mapped} ChEBI IDs to PubChem CID.")

    # Map PubChem CID to MeSH ID.
    cid2mesh = load_mapper_pubchem_cid_to_mesh_id()
    n_mapped = 0
    def map_pubchem_cid_to_mesh_id(row):
        global n_mapped

        if row['entity_type'] != 'chemical':
            return
        if 'pubchem_compound' not in row['external_ids']:
            return
        cid = row['external_ids']['pubchem_compound'][0]
        if cid in cid2mesh:
            row['external_ids']['mesh'] = [cid2mesh.loc[cid]]
            n_mapped += 1
    kg.entities._entities.progress_apply(map_pubchem_cid_to_mesh_id, axis=1)

    logger.info(f"Mapped {n_mapped} PubChem CID to MeSH ID.")

    kg.save("outputs/kg")
