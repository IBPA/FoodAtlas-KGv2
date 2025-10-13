import logging

import pandas as pd

from .._query import query_pubchem_compound
from ..utils import constants

logger = logging.getLogger(__name__)


def _create_chemical_entities_from_pubchem_compound(
    entities,
    records: pd.DataFrame,
):
    """Helper for creating chemical entities, which contain PubChem CIDs.

    Args:
        entities (Entities): Entities object.
        records (pd.DataFrame): Records from PubChem Compound.

    """
    logger.info("Start creating entities with PubChem CIDs...")

    def _parse_names(row):
        row["scientific_name"] = (
            row["IUPACName"].strip().lower() if not pd.isna(row["IUPACName"]) else None
        )

        row["synonyms"] = [s.strip().lower() for s in row["SynonymList"]]
        row["synonyms"] += (
            [row["scientific_name"]]
            if (
                row["scientific_name"] and row["scientific_name"] not in row["synonyms"]
            )
            else []
        )
        row["synonyms"] += [constants.get_lookup_key_by_id("pubchem_cid", row["CID"])]
        row["common_name"] = row["synonyms"][0]

        return row

    entities_new = records.copy()
    entities_new[entities.COLUMNS] = None
    entities_new = entities_new.apply(_parse_names, axis=1)

    # If CID already exists in the KG, just update the entity and skip.
    entities_skip = []
    for _, row in entities_new.iterrows():
        if (
            constants.get_lookup_key_by_id("pubchem_cid", row["CID"])
            in entities._lut_chemical
        ):
            entities_skip += [row["CID"]]
            eid = entities._lut_chemical[
                constants.get_lookup_key_by_id("pubchem_cid", row["CID"])
            ][0]
            for synonym in row["synonyms"]:
                if synonym not in entities._entities.at[eid, "synonyms"]:
                    entities._entities.at[eid, "synonyms"] += [synonym]
                    if synonym not in entities._lut_chemical:
                        entities._lut_chemical[synonym] = []
                    if eid not in entities._lut_chemical[synonym]:
                        entities._lut_chemical[synonym] += [eid]

    entities_new = entities_new[~entities_new["CID"].isin(entities_skip)]

    entities_new["external_ids"] = entities_new["CID"].apply(
        lambda x: {"pubchem_cid": x}
    )

    entities_new["foodatlas_id"] = [
        f"e{i}"
        for i in range(entities._curr_eid, entities._curr_eid + len(entities_new))
    ]
    entities._curr_eid += len(entities_new)

    entities_new["entity_type"] = "chemical"
    entities_new = entities_new[entities.COLUMNS].set_index("foodatlas_id")
    entities._entities = pd.concat([entities._entities, entities_new])
    entities._update_lut(entities_new)

    logger.info("Completed!")


def _create_chemical_entities_from_names(
    entities,
    names: list[str],
):
    """Helper for creating chemical entities, which do not have PubChem CIDs.

    Args:
        entities (Entities): Entities object.
        names (list[str]): Names of the entities.

    """
    logger.info("Start creating entities without PubChem CIDs...")

    entities_new_rows = []
    for name in names:
        if not entities.get_entity_ids("chemical", name):
            entities_new_rows += [
                {
                    "foodatlas_id": f"e{entities._curr_eid}",
                    "entity_type": "chemical",
                    "common_name": name,
                    "scientific_name": None,
                    "synonyms": [name],
                    "external_ids": {},
                }
            ]
            entities._curr_eid += 1

    entities_new = pd.DataFrame(entities_new_rows).set_index("foodatlas_id")
    entities._entities = pd.concat([entities._entities, entities_new])
    entities._update_lut(entities_new)

    logger.info("Completed!")


def create_chemical_entities(
    entities,
    entity_names_new: list[str],
):
    """Helper for creating chemical entities. For chemical entity, we first query
    PubChem Compound to see if there is an ID. If not, we create chemical entities
    without PubChem CIDs.

    Args:
        entities (Entities): Entities object.
        entity_names_new (list[str]): Names of the entities.

    """
    # Step 1. Query PubChem Compound to see if there is an ID.
    records_pubchem_compound = query_pubchem_compound(
        entity_names_new,
        entities.path_kg,
        entities.path_cache_dir,
    )
    _create_chemical_entities_from_pubchem_compound(entities, records_pubchem_compound)

    # Step 2. Create entities that are still new to the KG.
    _create_chemical_entities_from_names(entities, entity_names_new)
