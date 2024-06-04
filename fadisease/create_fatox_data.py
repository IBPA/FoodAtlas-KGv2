"""
Moving data from OpenFoodTox and ToxRefDB to the FA Disease database.
Requires create_factd_data to be run first.

Usage:
    python -u -m fadisease.create_fatox_data --data_dir=<data_dir> --log_level=<log_level> --output_dir=<output_dir>

Authors:
    Arielle Yoo - asmyoo@ucdavis.edu
"""

import os
import pandas as pd
import click
from ast import literal_eval
import re
import logging


from fadisease.utils.data import (
    split_column_into_length,
    load_oft_data,
    load_tvd_data,
    load_foodatlas_data,
    load_pubchem_to_cas_mapping,
    PUBCHEM_CID,
    CAS_ID,
    FA_CHEM_NAME,
    FA_ID,
    OFT_CHEMICAL_NAME,
    OFT_CAS_ID,
    TVD_CHEMICAL_NAME,
    TVD_CAS_ID,
    OFT_COLUMN_MAPPING,
    TVD_COLUMN_MAPPING,
    OFT_MAP_VALUES,
    TOXICITY_METADATA_COLUMNS,
    TOXICITY_METADATA_LOWERCASE_COLUMNS
)

from fadisease.utils.logging import get_logger, log_data
from fadisease.create_factd_data import get_max_fa_id


def filter_data(
    data: pd.DataFrame,
    fa_chem_lookup: dict,
    chem_name_col: str,
    cas_id_col: str
) -> pd.DataFrame:
    """
    Filter data to only include chemicals that are in the FoodAtlas database.

    Args:
        data (pd.DataFrame): Data
        fa_chem_lookup (dict): FoodAtlas chemical lookup
        chem_name_col (str): Column name for chemical name
        cas_id_col (str): Column name for CAS ID

    Returns:
        pd.DataFrame: Filtered data
    """
    # Filter data to only include chemicals that are in the FoodAtlas database
    data = data[data[chem_name_col].str.lower().isin(fa_chem_lookup.keys()) | data[cas_id_col].isin(
        fa_chem_lookup.keys())].reset_index(drop=True)
    return data


def update_oft_data(
        oft_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Update OpenFoodTox data.

    Args:
        oft_data (pd.DataFrame): OpenFoodTox data

    Returns:
        pd.DataFrame: Updated OpenFoodTox data
    """
    # Update ToxRefDB data
    oft_data["duration_unit"] = "days"
    oft_data = split_column_into_length(oft_data, "Route", ":", 2)
    oft_data["exposure_route"] = oft_data["Route_0"]
    oft_data["exposure_method"] = oft_data["Route_1"]
    oft_data = oft_data.map(lambda x: OFT_MAP_VALUES.get(
        x) if x in OFT_MAP_VALUES else x)
    oft_data["effect"] = oft_data[["Effect", "Toxicity"]].apply(
        lambda x: ",".join(x.fillna("")), axis=1)
    oft_data["effect"] = oft_data["effect"].apply(
        lambda x: re.sub(r'\s+', ' ', x))
    oft_data["effect"] = oft_data["effect"].apply(
        lambda x: x.strip(','))
    oft_data["effect"] = oft_data["effect"].apply(
        lambda x: x.strip())
    oft_data["source"] = "OpenFoodTox".lower()
    return oft_data


def update_tvd_data(
        tvd_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Update ToxRefDB data.

    Args:
        tvd_data (pd.DataFrame): ToxRefDB data

    Returns:
        pd.DataFrame: Updated ToxRefDB data
    """
    # Update ToxRefDB data
    tvd_data["specific_source"] = tvd_data[["subsource", "long_ref"]].apply(
        lambda x: ",".join(x.fillna("")), axis=1)
    tvd_data["specific_source"] = tvd_data["specific_source"].apply(
        lambda x: re.sub(r'\s+', ' ', x))
    tvd_data["specific_source"] = tvd_data["specific_source"].apply(
        lambda x: x.strip(','))
    tvd_data["specific_source"] = tvd_data["specific_source"].apply(
        lambda x: x.strip())
    tvd_data["source"] = "ToxRefDB".lower()
    return tvd_data


def create_tox_entities_metadata(
    fa_entities: pd.DataFrame,
    data: pd.DataFrame,
    data_col_mapping: dict,
    fa_chem_lookup: dict,
    chem_name_col: str,
    cas_id_col: str,
    metadata_max_count: int,
    logger: logging.Logger = None,
    chem_id_chem_name_to_fa_id_not_match_file: str = None,
) -> tuple:
    """
    Create metadata and entities for toxicity data.

    Args:
        fa_entities (pd.DataFrame): FoodAtlas entities
        data (pd.DataFrame): Data
        data_col_mapping (dict): Column mapping
        fa_chem_lookup (dict): FoodAtlas chemical lookup
        chem_name_col (str): Column name for chemical name
        cas_id_col (str): Column name for CAS ID
        metadata_max_count (int): Maximum count for metadata
        logger (logging.Logger, optional): Logger. Defaults to None.
        chem_id_chem_name_to_fa_id_not_match_file (str, optional): File to save chemicals that don't match. Defaults to None.

    Returns:
        tuple (pd.DataFrame, pd.DataFrame): The updated FoodAtlas entities and the metadata
    """
    data[FA_ID] = data.apply(
        lambda x: f'mt{x.name + metadata_max_count + 1}', axis=1)
    data[f"chem_{FA_ID}_fromname"] = data[chem_name_col].apply(
        lambda x: fa_chem_lookup[x.lower()] if x.lower() in fa_chem_lookup else None)
    data[f"chem_{FA_ID}_fromcas"] = data[cas_id_col].apply(
        lambda x: fa_chem_lookup[x] if x in fa_chem_lookup else None)
    data = data.rename(columns=data_col_mapping)
    if logger:
        logger.info(
            f"Number of unique chemicals in data from name: {data[f'chem_{FA_ID}_fromname'].nunique()}")
        logger.info(
            f"Number of unique chemicals in data from CAS: {data[f'chem_{FA_ID}_fromcas'].nunique()}")
        logger.info(
            f"Number where chem_{FA_ID}_fromname and chem_{FA_ID}_fromcas are not none: {data[(data[f'chem_{FA_ID}_fromname'].notnull()) & (data[f'chem_{FA_ID}_fromcas'].notnull())].shape[0]}")
        logger.info(
            f"Number where chem_{FA_ID}_fromname and chem_{FA_ID}_fromcas are not the same: {data[(data[f'chem_{FA_ID}_fromname'].notnull()) & (data[f'chem_{FA_ID}_fromcas'].notnull()) & (data[f'chem_{FA_ID}_fromname'] != data[f'chem_{FA_ID}_fromcas'])].shape[0]}")
    if chem_id_chem_name_to_fa_id_not_match_file:
        data[(data[f'chem_{FA_ID}_fromname'].notnull()) & (data[f'chem_{FA_ID}_fromcas'].notnull()) & (
            data[f'chem_{FA_ID}_fromname'] != data[f'chem_{FA_ID}_fromcas'])].to_csv(chem_id_chem_name_to_fa_id_not_match_file, sep="\t", index=False)
    data["chem_id"] = data[f"chem_{FA_ID}_fromcas"].combine_first(
        data[f"chem_{FA_ID}_fromname"])
    for col in TOXICITY_METADATA_COLUMNS:
        if col not in data.columns:
            data[col] = None
    metadata = data[TOXICITY_METADATA_COLUMNS + ["chem_id"]]
    metadata.loc[:, TOXICITY_METADATA_LOWERCASE_COLUMNS] = metadata.loc[:, TOXICITY_METADATA_LOWERCASE_COLUMNS].apply(
        lambda x: x.str.lower())
    data = data.groupby("chem_id").agg(
        metadata_ids_new=(FA_ID, list)).reset_index()
    if "metadata_ids" not in fa_entities.columns:
        fa_entities["metadata_ids"] = None
    fa_entities = fa_entities.merge(
        data, how="left", left_on=FA_ID, right_on="chem_id")
    fa_entities["metadata_ids"] = fa_entities["metadata_ids"].combine_first(
        fa_entities["metadata_ids_new"])
    fa_entities = fa_entities.drop(columns=["metadata_ids_new", "chem_id"])
    return fa_entities, metadata


@click.command()
@click.option("--data_dir", default="./data", help="The directory containing the data.")
@click.option("--log_level", default="debug", help="The level at which to log.")
@click.option("--output_dir", default="./outputs", help="The directory to save the output.")
def main(
    data_dir: str,
    log_level: str,
    output_dir: str
):
    logger = get_logger("create_fatox_data", log_level)

    # Load data
    oft_data = load_oft_data(data_dir)
    tvd_data = load_tvd_data(data_dir)
    logger.info(
        f"Number of chemicals in OpenFoodTox: {oft_data[OFT_CHEMICAL_NAME].nunique()}")
    logger.info(
        f"Number of metadata entries in OpenFoodTox: {oft_data.shape[0]}")
    logger.info(
        f"Number of chemicals in ToxRefDB: {tvd_data[TVD_CHEMICAL_NAME].nunique()}")
    logger.info(f"Number of metadata entries in ToxRefDB: {tvd_data.shape[0]}")

    kg_dir = os.path.join(output_dir, "kg_updated")
    if not os.path.exists(kg_dir):
        logger.error(
            f"Directory {kg_dir} does not exist. Please run create_factd_data first.")
        return

    # Load data
    fa_entities = load_foodatlas_data(kg_dir, "folder_entities")
    # log_data(logger, fa_entities, "fa_entities")
    # logger.info(f"tail of fa_entities: {fa_entities.tail()}")

    # # Load PubChem to CAS mapping
    # pubchem_to_cas = load_pubchem_to_cas_mapping(output_dir, logger)
    # # log_data(logger, pubchem_to_cas, "pubchem_to_cas")
    # pubchem_to_cas.to_csv(os.path.join(output_dir, "pubchem_to_cas.csv"))
    # # check length of pubchem_to_cas CAS_ID
    # pubchem_to_cas["CAS_ID_len"] = pubchem_to_cas[CAS_ID].apply(
    #     lambda x: len(x) if x != None else 0)
    # logger.info(
    #     f"Length of CAS_ID in pubchem_to_cas: {pubchem_to_cas['CAS_ID_len'].value_counts()}")
    # pubchem_to_cas = pubchem_to_cas.set_index(PUBCHEM_CID)[CAS_ID].to_dict()

    # I just realized that I don't need to use the PubChem to CAS mapping
    # CAS ID's are already in FoodAtlas entity synonyms

    fa_chem_lookup = load_foodatlas_data(data_dir, "chem_lookup")
    fa_chem_lookup = fa_chem_lookup.set_index(
        FA_CHEM_NAME)[FA_ID].to_dict()

    # Filter OpenFoodTox data to only include chemicals that are in the FoodAtlas database
    oft_data = filter_data(oft_data, fa_chem_lookup,
                           OFT_CHEMICAL_NAME, OFT_CAS_ID)
    logger.info(
        f"Number of chemicals in OpenFoodTox after filtering: {oft_data[OFT_CHEMICAL_NAME].nunique()}")
    logger.info(
        f"Number of metadata entries in OpenFoodTox after filtering: {oft_data.shape[0]}")

    # Filter ToxRefDB data to only include chemicals that are in the FoodAtlas database
    tvd_data = filter_data(tvd_data, fa_chem_lookup,
                           TVD_CHEMICAL_NAME, TVD_CAS_ID)
    logger.info(
        f"Number of chemicals in ToxRefDB after filtering: {tvd_data[TVD_CHEMICAL_NAME].nunique()}")
    logger.info(
        f"Number of metadata entries in ToxRefDB after filtering: {tvd_data.shape[0]}")

    tvd_data = update_tvd_data(tvd_data)
    oft_data = update_oft_data(oft_data)
    # add to FoodAtlas entities
    tsv_not_match_file = os.path.join(
        kg_dir, "tvd_chem_id_chem_name_to_fa_id_not_match.tsv")
    fa_entities, metadata_tvd = create_tox_entities_metadata(
        fa_entities, tvd_data, TVD_COLUMN_MAPPING, fa_chem_lookup, TVD_CHEMICAL_NAME, TVD_CAS_ID, 0, logger, tsv_not_match_file)
    metadata_max_count = metadata_tvd.shape[0]
    logger.info("TVDDATA")
    logger.info(
        f"Number of unique chemicals in metadata by name: {metadata_tvd['chemical_name'].nunique()}")
    logger.info(
        f"Number of metadata entries in metadata by name: {metadata_tvd.groupby('chemical_name').size().reset_index(name='count')['count'].value_counts()}")
    logger.info(
        f"Number of unique chemicals in metadata by chem_id: {metadata_tvd['chem_id'].nunique()}")
    logger.info(
        f"Number of metadata entries in metadata by chem_id: {metadata_tvd.groupby('chem_id').size().reset_index(name='count')['count'].value_counts()}")
    # metadata_oft = None
    oft_not_match_file = os.path.join(
        kg_dir, "oft_chem_id_chem_name_to_fa_id_not_match.tsv")
    fa_entities, metadata_oft = create_tox_entities_metadata(
        fa_entities, oft_data, OFT_COLUMN_MAPPING, fa_chem_lookup, OFT_CHEMICAL_NAME, OFT_CAS_ID, metadata_max_count, logger, oft_not_match_file)
    logger.info("OFTDATA")
    logger.info(
        f"Number of unique chemicals in metadata by name: {metadata_oft['chemical_name'].nunique()}")
    logger.info(
        f"Number of metadata entries in metadata by name: {metadata_oft.groupby('chemical_name').size().reset_index(name='count')['count'].value_counts()}")
    logger.info(
        f"Number of unique chemicals in metadata by chem_id: {metadata_oft['chem_id'].nunique()}")
    logger.info(
        f"Number of metadata entries in metadata by chem_id: {metadata_oft.groupby('chem_id').size().reset_index(name='count')['count'].value_counts()}")
    metadata = pd.concat([metadata_tvd, metadata_oft], ignore_index=True)
    metadata = metadata.reset_index(drop=True)
    logger.info("METADATA FINAL")
    logger.info(
        f"Number of metadata entries in metadata: {metadata.shape[0]}")
    logger.info(
        f"Number of unique chemicals in metadata by name: {metadata['chemical_name'].nunique()}")
    logger.info(
        f"Number of metadata entries in metadata by name: {metadata.groupby('chemical_name').size().reset_index(name='count')['count'].value_counts()}")
    logger.info(
        f"Number of unique chemicals in metadata by chem_id: {metadata['chem_id'].nunique()}")
    logger.info(
        f"Number of metadata entries in metadata by chem_id: {metadata.groupby('chem_id').size().reset_index(name='count')['count'].value_counts()}")
    # drop chem_id column
    metadata = metadata.drop(columns=["chem_id"])
    # only get entities that are chemicals and only save FA_ID and metadata_ids
    fa_entities = fa_entities[fa_entities["entity_type"] == "chemical"]
    fa_entities = fa_entities[[FA_ID, "metadata_ids"]]
    # drop null metadata_ids
    fa_entities = fa_entities[fa_entities["metadata_ids"].notnull()]
    fa_entities.to_csv(os.path.join(
        kg_dir, "entities_tox.tsv"), index=False, sep="\t")
    metadata.to_csv(os.path.join(kg_dir, "metadata_tox.tsv"),
                    index=False, sep="\t")


if __name__ == "__main__":
    main()
