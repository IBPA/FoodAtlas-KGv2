"""
Moving data from CTD to FoodAtlas just from reading files.
Requires PubChemToCTD.txt file.

Usage:
    python -u -m fadisease.create_factd_data --data_dir=<data_dir> --log_level=<log_level> --output_dir=<output_dir>

Authors:
    Arielle Yoo - asmyoo@ucdavis.edu
"""

import logging
import os
import re

import click
import pandas as pd

from .utils.data import (
    CTD_ALTID_MAPPING,
    CTD_CHEMICAL_ID,
    CTD_CHEMICAL_NAME,
    CTD_DIRECTEVIDENCE,
    CTD_DIRECTEVIDENCE_MAPPING,
    CTD_DISEASE_ID,
    CTD_PUBMED_IDS,
    CTD_REVERSE_ALTID_MAPPING,
    ENTITY_TYPE,
    EXTERNAL_IDS,
    FA_CHEM_NAME,
    FA_ID,
    MESH_IDS,
    PMCID,
    PUBCHEM_CID,
    PUBMED_IDS,
    adjust_FA_entities_data,
    load_ctd_data,
    load_foodatlas_data,
    load_pmid_to_pmcid_mapping,
    load_pubchem_to_ctd_mapping,
)
from .utils.logging import get_logger


def add_mesh_ids_to_fa_chemicals(
    fa_chemicals: pd.DataFrame, pubchem_to_ctd: dict
) -> pd.DataFrame:
    """
    Add MeSH IDs to the FoodAtlas chemicals.

    Args:
        fa_chemicals (pd.DataFrame): The FoodAtlas chemicals.
        pubchem_to_ctd (dict): The mapping from PubChem to CTD.

    Returns:
        pd.DataFrame: The updated FoodAtlas chemicals.
    """
    fa_chemicals.loc[fa_chemicals[ENTITY_TYPE] == "chemical", EXTERNAL_IDS] = (
        fa_chemicals.loc[fa_chemicals[ENTITY_TYPE] == "chemical", EXTERNAL_IDS].apply(
            lambda x: {**x, MESH_IDS: pubchem_to_ctd[x[PUBCHEM_CID]]}
            if (PUBCHEM_CID in x and x[PUBCHEM_CID] in pubchem_to_ctd)
            else x
        )
    )
    return fa_chemicals


def get_max_fa_id(fa_entities: pd.DataFrame) -> int:
    """
    Get the maximum entity id in the FoodAtlas entities.

    Args:
        fa_entities (pd.DataFrame): The FoodAtlas entities.

    Returns:
        int: The maximum entity id.
    """
    fa_entities[FA_ID + "_int"] = fa_entities[FA_ID].str.extract(r"(\d+)").astype(int)
    fa_max_entity_id = fa_entities[FA_ID + "_int"].max()
    fa_entities.drop(columns=[FA_ID + "_int"], inplace=True)
    return fa_max_entity_id


def filter_ctd_chemdis(
    ctd_chemdis: pd.DataFrame,
    fa_chemicals: pd.DataFrame,
    pubchem_to_ctd: dict,
    fa_chem_lookup: dict,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Filter the CTD chemical-disease associations.
    Chemical-Disease without DirectEvidence are removed.
    Chemicals that are not in FoodAtlas are removed.

    Args:
        ctd_chemdis (pd.DataFrame): The CTD chemical-disease associations.
        fa_chemicals (pd.DataFrame): The FoodAtlas chemicals. Unused right now.
        pubchem_to_ctd (dict): The mapping from PubChem to CTD.
        fa_chem_lookup (dict): The lookup from FoodAtlas chemical name to entity id.
        logger (logging.Logger): The logger to use.

    Returns:
        pd.DataFrame: The filtered CTD chemical-disease associations.
    """
    ctd_chemdis = ctd_chemdis[ctd_chemdis[CTD_DIRECTEVIDENCE].notnull()].reset_index(
        drop=True
    )
    # get all chemical ids in pubchem_to_ctd
    mesh_ids = [mesh_id for mesh_ids in pubchem_to_ctd.values() for mesh_id in mesh_ids]
    mesh_ids_set = set(mesh_ids)
    ctd_chemdis = ctd_chemdis[
        ctd_chemdis[CTD_CHEMICAL_ID].isin(mesh_ids_set)
        | ctd_chemdis[CTD_CHEMICAL_NAME].str.lower().isin(fa_chem_lookup.keys())
    ].reset_index(drop=True)
    return ctd_chemdis


def get_disease_ids_from_alt_disease_ids(row: pd.Series) -> dict:
    """
    Get the disease ids from the AltDiseaseIDs.

    Args:
        row (pd.Series): The row of the CTD diseases.
        alt_disease_ids_types (set): The types of the AltDiseaseIDs.

    Returns:
        dict: The disease ids.
    """
    alt_disease_ids = row["AltDiseaseIDs"]
    alt_disease_ids = [item.split(":") for item in alt_disease_ids]
    alt_disease_ids = [{item[0]: ":".join(item[1:])} for item in alt_disease_ids]
    # combine all alt_disease_ids by key into one dict
    # all alt_disease_ids are stored in a list
    alt_disease_ids_full = {}
    for d in alt_disease_ids:
        for key, value in d.items():
            if key not in alt_disease_ids_full:
                alt_disease_ids_full[key] = [value]
            else:
                alt_disease_ids_full[key].append(value)
    # use mapping to change keys
    alt_disease_ids_full = {
        CTD_ALTID_MAPPING[key]: value for key, value in alt_disease_ids_full.items()
    }
    # change values to int if possible
    for key, value in alt_disease_ids_full.items():
        alt_disease_ids_full[key] = [int(x) if x.isdigit() else x for x in value]
    alt_disease_ids_full = {**row[EXTERNAL_IDS], **alt_disease_ids_full}
    return alt_disease_ids_full


def create_disease_entities(
    fa_entities: pd.DataFrame,
    ctd_diseases: pd.DataFrame,
    ctd_chemdis: pd.DataFrame,
    fa_max_entity_id: int,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Create disease entities in FoodAtlas. Assumes currently no disease entities in FoodAtlas.

    Args:
        fa_entities (pd.DataFrame): The FoodAtlas entities.
        ctd_diseases (pd.DataFrame): The CTD diseases.
        ctd_chemdis (pd.DataFrame): The filtered CTD chemical-disease associations
        fa_max_entity_id (int): The maximum entity id in FoodAtlas.
        logger (logging.Logger): The logger to use.

    Returns:
        pd.DataFrame: The updated FoodAtlas entities.
    """
    # get all diseases in ctd_chemdis by DiseaseID
    ctd_diseases = ctd_diseases[
        ctd_diseases[CTD_DISEASE_ID].isin(ctd_chemdis[CTD_DISEASE_ID])
    ].reset_index(drop=True)
    # fill in disease entities
    ctd_diseases[FA_ID] = ctd_diseases.apply(
        lambda x: f"e{fa_max_entity_id + x.name + 1}", axis=1
    )
    ctd_diseases[ENTITY_TYPE] = "disease"
    ctd_diseases["common_name"] = ctd_diseases["DiseaseName"].str.lower()
    # make none for scientific_name
    ctd_diseases["scientific_name"] = None
    ctd_diseases["synonyms"] = ctd_diseases["Synonyms"].apply(
        lambda x: [item.lower() for item in x] if isinstance(x, list) else None
    )
    ctd_diseases[EXTERNAL_IDS] = ctd_diseases[CTD_DISEASE_ID].apply(
        lambda x: {CTD_ALTID_MAPPING[x.split(":")[0]]: [":".join(x.split(":")[1:])]}
    )
    ctd_diseases[EXTERNAL_IDS] = ctd_diseases.apply(
        lambda x: get_disease_ids_from_alt_disease_ids(x), axis=1
    )
    # add disease entities to fa_entities
    fa_entities = pd.concat(
        [fa_entities, ctd_diseases], join="inner", ignore_index=True
    )
    return fa_entities


def create_mapping_ctd_to_fa(
    fa_entities: pd.DataFrame, chem=True, logger: logging.Logger = None
) -> dict:
    """
    Create mapping from CTD ChemicalID to FoodAtlas entity id.

    Args:
        fa_entities (pd.DataFrame): The FoodAtlas entities.
        chem (bool): Whether to create mapping for chemicals or diseases.
        logger (logging.Logger): The logger to use.

    Returns:
        dict: The mapping from CTD ChemicalID to FoodAtlas entity id.
    """
    fa_entities, check_ids = adjust_FA_entities_data(fa_entities)
    # map list mesh_ids to fa_id
    ctd_id_to_fa_id = {}
    for i, row in fa_entities.iterrows():
        if MESH_IDS in row[EXTERNAL_IDS]:
            if isinstance(row[EXTERNAL_IDS][MESH_IDS], list):
                for mesh_id in row[EXTERNAL_IDS][MESH_IDS]:
                    if chem:
                        ctd_id = mesh_id
                    else:
                        ctd_id = f"{CTD_REVERSE_ALTID_MAPPING[MESH_IDS]}:{mesh_id}"
                    if ctd_id not in ctd_id_to_fa_id:
                        ctd_id_to_fa_id[ctd_id] = row[FA_ID]
                    else:
                        # check if fa_id is the same
                        if ctd_id_to_fa_id[ctd_id] != row[FA_ID]:
                            if logger:
                                logger.error(
                                    f"mesh_id {mesh_id} already exists with fa_id {ctd_id_to_fa_id[ctd_id]}, new fa_id {row[FA_ID]}"
                                )
            else:
                if logger:
                    logger.error(f"mesh_ids is not a list for {row[FA_ID]}")
                mesh_id = row[EXTERNAL_IDS][MESH_IDS]
                if chem:
                    ctd_id = mesh_id
                else:
                    ctd_id = f"{CTD_REVERSE_ALTID_MAPPING[MESH_IDS]}:{mesh_id}"
                # ctd_id = f"{CTD_REVERSE_ALTID_MAPPING[MESH_IDS]}:{mesh_id}"
                if ctd_id not in ctd_id_to_fa_id:
                    ctd_id_to_fa_id[ctd_id] = row[FA_ID]
                else:
                    # check if fa_id is the same
                    if ctd_id_to_fa_id[ctd_id] != row[FA_ID]:
                        if logger:
                            logger.error(
                                f"mesh_id {mesh_id} already exists with fa_id {ctd_id_to_fa_id[ctd_id]}, new fa_id {row[FA_ID]}"
                            )
        if CTD_ALTID_MAPPING["OMIM"] in row[EXTERNAL_IDS]:
            if isinstance(row[EXTERNAL_IDS][CTD_ALTID_MAPPING["OMIM"]], list):
                for omim_id in row[EXTERNAL_IDS][CTD_ALTID_MAPPING["OMIM"]]:
                    ctd_id = f"{CTD_REVERSE_ALTID_MAPPING[CTD_ALTID_MAPPING['OMIM']]}:{omim_id}"
                    if ctd_id not in ctd_id_to_fa_id:
                        ctd_id_to_fa_id[ctd_id] = row[FA_ID]
                    else:
                        # check if fa_id is the same
                        if ctd_id_to_fa_id[ctd_id] != row[FA_ID]:
                            if logger:
                                logger.error(
                                    f"omim_id {omim_id} already exists with fa_id {ctd_id_to_fa_id[ctd_id]}, new fa_id {row[FA_ID]}"
                                )
            else:
                if logger:
                    logger.error(f"omim_ids is not a list for {row[FA_ID]}")
                omim_id = row[EXTERNAL_IDS][CTD_ALTID_MAPPING["OMIM"]]
                ctd_id = (
                    f"{CTD_REVERSE_ALTID_MAPPING[CTD_ALTID_MAPPING['OMIM']]}:{omim_id}"
                )
                if ctd_id not in ctd_id_to_fa_id:
                    ctd_id_to_fa_id[ctd_id] = row[FA_ID]
                else:
                    # check if fa_id is the same
                    if ctd_id_to_fa_id[ctd_id] != row[FA_ID]:
                        if logger:
                            logger.error(
                                f"omim_id {omim_id} already exists with fa_id {ctd_id_to_fa_id[ctd_id]}, new fa_id {row[FA_ID]}"
                            )
    return ctd_id_to_fa_id


def create_disease_triplets_metadata(
    fa_triplets: pd.DataFrame,
    ctd_chemdis: pd.DataFrame,
    fa_entities: pd.DataFrame,
    fa_max_triplet_id: int,
    fa_chem_lookup: dict,
    logger: logging.Logger = None,
    chem_id_pubchem_name_to_fa_id_not_match_file: str = None,
) -> tuple:
    """
    Create disease triplets and metadata in FoodAtlas.

    Args:
        fa_triplets (pd.DataFrame): The FoodAtlas triplets.
        fa_metadata (pd.DataFrame): The FoodAtlas metadata.
        ctd_chemdis (pd.DataFrame): The filtered CTD chemical-disease associations.
        fa_entities (pd.DataFrame): The FoodAtlas entities that have been updated with disease entities.
        fa_max_triplet_id (int): The maximum triplet id in FoodAtlas.
        fa_chem_lookup (dict): The lookup from FoodAtlas chemical name to entity id.
        logger (logging.Logger): The logger to use.
        chem_id_pubchem_name_to_fa_id_not_match_file (str): The file to save the
            entities where converting using PubChem CID and chemical name to FoodAtlas entity id did not match.

    Returns:
        tuple (pd.DataFrame, pd.DataFrame): The updated FoodAtlas triplets and metadata.
    """
    # create mapping from CTD ChemicalID to FoodAtlas entity id
    fa_entities_chemicals = fa_entities[
        fa_entities[ENTITY_TYPE] == "chemical"
    ].reset_index(drop=True)
    mesh_ids_to_fa_id_chemicals = create_mapping_ctd_to_fa(
        fa_entities_chemicals, True, logger
    )
    # create mapping from CTD DiseaseID to FoodAtlas entity id
    fa_entities_diseases = fa_entities[
        fa_entities[ENTITY_TYPE] == "disease"
    ].reset_index(drop=True)
    mesh_ids_to_fa_id_diseases = create_mapping_ctd_to_fa(
        fa_entities_diseases, False, logger
    )
    # create triplets and metadata
    ctd_chemdis["head_id"] = ctd_chemdis[CTD_CHEMICAL_ID].apply(
        lambda x: mesh_ids_to_fa_id_chemicals[x]
        if x in mesh_ids_to_fa_id_chemicals
        else None
    )
    ctd_chemdis["head_id_name"] = (
        ctd_chemdis[CTD_CHEMICAL_NAME].str.lower().map(fa_chem_lookup)
    )
    if logger:
        logger.info(f"Number of unique head_id: {ctd_chemdis['head_id'].nunique()}")
        logger.info(
            f"Number of unique head_id_name: {ctd_chemdis['head_id_name'].nunique()}"
        )
        # check if head_id and head_id_name are the same for all when neither are None
        logger.info(
            f"Number of head_id and head_id_name that are not None: {ctd_chemdis[(ctd_chemdis['head_id'].notnull()) & (ctd_chemdis['head_id_name'].notnull())].shape[0]}"
        )
        logger.info(
            f"Number of head_id and head_id_name that are not the same: {ctd_chemdis[(ctd_chemdis['head_id'].notnull()) & (ctd_chemdis['head_id_name'].notnull()) & (ctd_chemdis['head_id'] != ctd_chemdis['head_id_name'])].shape[0]}"
        )
    if chem_id_pubchem_name_to_fa_id_not_match_file:
        # save entities where head_id and head_id_name do not match
        ctd_chemdis_not_match = ctd_chemdis[
            (ctd_chemdis["head_id"].notnull())
            & (ctd_chemdis["head_id_name"].notnull())
            & (ctd_chemdis["head_id"] != ctd_chemdis["head_id_name"])
        ]
        ctd_chemdis_not_match.to_csv(
            chem_id_pubchem_name_to_fa_id_not_match_file, index=False, sep="\t"
        )
    # ctd_chemdis["head_id"] = ctd_chemdis.apply(
    #     lambda x: x["head_id"] if pd.notnull(x["head_id"]) else x["head_id_name"], axis=1)
    ctd_chemdis["head_id"] = ctd_chemdis["head_id"].combine_first(
        ctd_chemdis["head_id_name"]
    )
    # ctd_chemdis["head_id"] = ctd_chemdis["head_id_name"].combine_first(
    #     ctd_chemdis["head_id"])
    ctd_chemdis["relationship_id"] = ctd_chemdis[CTD_DIRECTEVIDENCE].apply(
        lambda x: CTD_DIRECTEVIDENCE_MAPPING[x]
    )
    ctd_chemdis["tail_id"] = ctd_chemdis[CTD_DISEASE_ID].apply(
        lambda x: mesh_ids_to_fa_id_diseases[x]
    )
    if logger:
        logger.info(f"ctd_chemdis unique chemicals: {ctd_chemdis['head_id'].nunique()}")
        logger.info(
            f"ctd_chemdis relationship counts: {ctd_chemdis['relationship_id'].value_counts()}"
        )
        logger.info(f"ctd_chemdis unique diseases: {ctd_chemdis['tail_id'].nunique()}")
        logger.info(f"ctd_chemdis number of triplets: {ctd_chemdis.shape[0]}")
        logger.info(
            f"ctd_chemdis number of unique triplets: {ctd_chemdis[['head_id', 'relationship_id', 'tail_id']].drop_duplicates().shape[0]}"
        )
        # check number of triplets by head_id
        logger.info(
            f"ctd_chemdis number of triplets by head_id:\n{ctd_chemdis.groupby('head_id').size().value_counts().sort_index()}"
        )
        # check number of triplets by CTD_CHEMICAL_ID
        logger.info(
            f"ctd_chemdis number of triplets by CTD_CHEMICAL_ID:\n{ctd_chemdis.groupby(CTD_CHEMICAL_ID).size().value_counts().sort_index()}"
        )
    # explode CTD_PUBMED_IDS so that each pubmed_id is in a separate row
    ctd_chemdis = ctd_chemdis.explode(CTD_PUBMED_IDS).reset_index(drop=True)
    ctd_chemdis["metadata_ids"] = ctd_chemdis.apply(
        lambda x: [f"md{x.name + 1}"], axis=1
    )
    # create metadata for triplets
    ctd_chemdis_metadata = pd.DataFrame()
    ctd_chemdis_metadata[FA_ID] = ctd_chemdis["metadata_ids"].apply(lambda x: x[0])
    ctd_chemdis_metadata["source"] = "ctd"
    pubmed_to_pmcid = load_pmid_to_pmcid_mapping()
    pubmed_to_pmcid = pubmed_to_pmcid.set_index(PUBMED_IDS)[PMCID].to_dict()
    # only get number from pmcid using regex
    # pubmed_to_pmcid = {key: [re.search(r'\d+', value).group() if pd.notnull(value) else None]
    #                    for key, value in pubmed_to_pmcid.items()}
    ctd_chemdis_metadata["reference"] = ctd_chemdis[CTD_PUBMED_IDS].apply(
        lambda x: {
            PUBMED_IDS: x,
            PMCID: pubmed_to_pmcid[x] if x in pubmed_to_pmcid else None,
        }
    )
    # remove keys in reference that are None
    ctd_chemdis_metadata["reference"] = ctd_chemdis_metadata["reference"].apply(
        lambda x: {key: value for key, value in x.items() if value is not None}
    )
    # combine unique triplets and combine metadata id list
    ctd_chemdis = (
        ctd_chemdis.groupby(["head_id", "relationship_id", "tail_id"])
        .agg(metadata_ids=("metadata_ids", lambda x: sum(x, [])))
        .reset_index()
    )
    # sort by lowest value of int within list of strings in metadata_ids
    ctd_chemdis["metadata_ids_min"] = ctd_chemdis["metadata_ids"].apply(
        lambda x: min([int(re.search(r"\d+", i).group()) for i in x])
    )
    ctd_chemdis = ctd_chemdis.sort_values(by=["metadata_ids_min"]).reset_index(
        drop=True
    )
    ctd_chemdis[FA_ID] = ctd_chemdis.apply(
        lambda x: f"t{fa_max_triplet_id + x.name + 1}", axis=1
    )
    if logger:
        # should be equal to number of unique triplets
        # logger.info(
        #     f"ctd_chemdis number of triplets after grouping: {ctd_chemdis.shape[0]}")
        # logger.info(
        #     f"ctd_chemdis number of unique triplets after grouping: {ctd_chemdis[['head_id', 'relationship_id', 'tail_id']].drop_duplicates().shape[0]}")
        logger.info(
            f"ctd_chemdis relationship counts after grouping: {ctd_chemdis['relationship_id'].value_counts()}"
        )
        ctd_chemdis["metadata_ids_len"] = ctd_chemdis["metadata_ids"].apply(
            lambda x: len(x)
        )
        logger.info(
            f"ctd_chemdis metadata_ids_len: {ctd_chemdis['metadata_ids_len'].value_counts()}"
        )
    # add triplets and metadata to fa_triplets and fa_metadata
    fa_triplets = pd.concat([fa_triplets, ctd_chemdis], join="inner", ignore_index=True)

    return fa_triplets, ctd_chemdis_metadata


@click.command()
@click.option("--data_dir", default="./data", help="The directory containing the data.")
@click.option("--log_level", default="debug", help="The level at which to log.")
@click.option(
    "--output_dir", default="./outputs", help="The directory to save the output."
)
def main(data_dir: str, log_level: str, output_dir: str):
    logger = get_logger("create_factd_data", log_level)

    # Load data
    ctd_chemdis = load_ctd_data(data_dir)
    ctd_chemicals = load_ctd_data(data_dir, "chem")
    ctd_diseases = load_ctd_data(data_dir, "disease")
    fa_entities = load_foodatlas_data(data_dir)
    fa_metadata = load_foodatlas_data(data_dir, "metadata")
    fa_triplets = load_foodatlas_data(data_dir, "triplets")
    fa_chem_lookup = load_foodatlas_data(data_dir, "chem_lookup")
    fa_chem_lookup = fa_chem_lookup.set_index(FA_CHEM_NAME)[FA_ID].to_dict()
    pubchem_to_ctd = load_pubchem_to_ctd_mapping(output_dir)
    pubchem_to_ctd = pubchem_to_ctd.set_index(PUBCHEM_CID)[CTD_CHEMICAL_ID].to_dict()

    # create kg folder
    kg_dir = os.path.join(output_dir, "kg_updated")
    if not os.path.exists(kg_dir):
        os.makedirs(kg_dir)

    # update current chemical entities in FoodAtlas with MeSH IDs from CTD
    # add MeSH IDs to fa_chemicals
    fa_entities = add_mesh_ids_to_fa_chemicals(fa_entities, pubchem_to_ctd)

    # save updated fa_entities
    fa_entities.to_csv(
        os.path.join(kg_dir, "entities_meshids_tmp.tsv"), sep="\t", index=False
    )
    logger.info("Updated fa_entities with mesh ids saved")

    # filter ctd_chemdis
    ctd_chemdis = filter_ctd_chemdis(
        ctd_chemdis, fa_entities, pubchem_to_ctd, fa_chem_lookup, logger
    )
    logger.info("Filtered ctd_chemdis")
    logger.info(
        f"Number of unique chemicals in CTD: {ctd_chemdis[CTD_CHEMICAL_ID].nunique()}"
    )
    logger.info(
        f"Number of unique diseases in CTD: {ctd_chemdis[CTD_DISEASE_ID].nunique()}"
    )

    # create disease entities in FoodAtlas
    fa_max_entity_id = get_max_fa_id(fa_entities)
    logger.info(f"fa_max_entity_id: {fa_max_entity_id}")
    fa_entities = create_disease_entities(
        fa_entities, ctd_diseases, ctd_chemdis, fa_max_entity_id, logger
    )
    # save entities
    fa_entities.to_csv(os.path.join(kg_dir, "entities.tsv"), sep="\t", index=False)
    logger.info("Disease entities created and saved")

    # update triplets and create new metadata
    fa_max_triplet_id = get_max_fa_id(fa_triplets)
    logger.info(f"fa_max_triplet_id: {fa_max_triplet_id}")
    chem_id_pubchem_name_to_fa_id_not_match_file = os.path.join(
        kg_dir, "chem_id_pubchem_name_to_fa_id_not_match.tsv"
    )
    fa_triplets, fa_metadata = create_disease_triplets_metadata(
        fa_triplets,
        ctd_chemdis,
        fa_entities,
        fa_max_triplet_id,
        fa_chem_lookup,
        logger,
        chem_id_pubchem_name_to_fa_id_not_match_file,
    )
    # save triplets and metadata
    fa_triplets.to_csv(os.path.join(kg_dir, "triplets.tsv"), sep="\t", index=False)
    fa_metadata.to_csv(
        os.path.join(kg_dir, "metadata_diseases.tsv"), sep="\t", index=False
    )
    logger.info("Triplets and metadata created and saved")


if __name__ == "__main__":
    main()
