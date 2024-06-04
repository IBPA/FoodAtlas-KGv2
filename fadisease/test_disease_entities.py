"""
Small test for entities in disease data

Usage:
    python -u -m fadisease.test_disease_entities --data_dir=<data_dir> --log_level=<log_level> --output_dir=<output_dir>

Authors:
    Arielle Yoo - asmyoo@ucdavis.edu
"""

import os
import pandas as pd
import click
from ast import literal_eval

from fadisease.utils.data import (
    EXTERNAL_IDS,
    MESH_IDS,
)
from fadisease.utils.logging import get_logger


@click.command()
@click.option("--data_dir", default="./data", help="The directory containing the data.")
@click.option("--log_level", default="debug", help="The level at which to log.")
@click.option("--output_dir", default="./outputs", help="The directory to save the output.")
def main(
    data_dir: str,
    log_level: str,
    output_dir: str
):

    logger = get_logger(log_level=log_level,
                        logger_name="test_disease_entities")

    kg_dir = os.path.join(output_dir, "kg_updated")
    if not os.path.exists(kg_dir):
        logger.error(f"Directory {kg_dir} does not exist.")
        return

    # Load the data
    fa_entities = pd.read_csv(os.path.join(kg_dir, "entities.tsv"), sep='\t')

    # get only diseases
    fa_diseases = fa_entities[fa_entities["entity_type"]
                              == "disease"].reset_index(drop=True)
    logger.info(
        f"Number of diseases in FoodAtlas: {fa_diseases['foodatlas_id'].nunique()}")

    fa_diseases[EXTERNAL_IDS] = fa_diseases[EXTERNAL_IDS].apply(
        lambda x: literal_eval(x) if pd.notnull(x) else {})

    # check length of mesh_ids in external_ids
    fa_diseases[f"{MESH_IDS}_len"] = fa_diseases[EXTERNAL_IDS].apply(
        lambda x: len(x[MESH_IDS]) if MESH_IDS in x else 0)
    # check length of omim_ids in external_ids
    fa_diseases["omim_ids_len"] = fa_diseases[EXTERNAL_IDS].apply(
        lambda x: len(x["omim_ids"]) if "omim_ids" in x else 0)

    logger.info(
        f"values for mesh_ids_len: {fa_diseases['mesh_ids_len'].value_counts()}")
    logger.info(
        f"values for omim_ids_len: {fa_diseases['omim_ids_len'].value_counts()}")

    # check for the rows with no mesh_ids
    no_mesh_ids = fa_diseases[fa_diseases["mesh_ids_len"] == 0]
    logger.info(
        f"Number of diseases with no mesh_ids: {no_mesh_ids.shape[0]}")
    logger.info(
        f"values for omim_ids_len in no_mesh_ids: {no_mesh_ids['omim_ids_len'].value_counts()}")

    # check if there are overlapping mesh_ids between unique pubchem_cid in PubChemToCTD
    pubchem_to_ctd = pd.read_csv(os.path.join(
        output_dir, "PubChemToCTD.txt"), sep='\t', header=None)
    pubchem_to_ctd.columns = ["pubchem_cid", "ctd_chemical_ids"]
    # check duplicate ctd_chemical_ids
    pubchem_to_ctd_check = pubchem_to_ctd.groupby("ctd_chemical_ids")[
        "pubchem_cid"].apply(list).reset_index(name="pubchem_cid")
    pubchem_to_ctd_check["len_pubchem_cid"] = pubchem_to_ctd_check["pubchem_cid"].apply(
        lambda x: len(x))
    logger.info(
        f"head of pubchem_to_ctd_check: {pubchem_to_ctd_check.head()}")
    logger.info(
        f"values for len_pubchem_cid: {pubchem_to_ctd_check['len_pubchem_cid'].value_counts()}")

    return


if __name__ == "__main__":
    main()
