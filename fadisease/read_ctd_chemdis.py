"""
This script reads the CTD_chemicals_diseases.csv file and extracts the chemical-disease associations.

Usage:
    python -u -m fadisease.read_ctd_chemdis --data_dir=<data_dir> --log_level=<log_level> --output_dir=<output_dir>

Authors:
    Arielle Yoo - asmyoo@ucdavis.edu
"""
import os
import pandas as pd
import click

from fadisease.utils.data import load_ctd_data
from fadisease.utils.logging import get_logger


def log_relevant_info(data, logger, check_evidence=True):
    """
    Log relevant information about the data.

    Args:
        data (pd.DataFrame): The data to log information about.
        logger (logging.Logger): The logger to use.
        check_evidence (bool): Whether to check the evidence column. Default is True.

    Returns:
        None
    """
    num_chems = data["ChemicalID"].nunique()
    num_diseases = data["DiseaseID"].nunique()
    if check_evidence:
        num_associations = data[["ChemicalID", "DiseaseID", "DirectEvidence"]
                                ].drop_duplicates().shape[0]
    else:
        num_associations = data[["ChemicalID", "DiseaseID"]
                                ].drop_duplicates().shape[0]
    logger.info(
        f"Number of unique chemicals: {num_chems}, diseases: {num_diseases}, associations: {num_associations}")
    num_chems_with_chemID = data[data["ChemicalID"].notnull(
    )]["ChemicalID"].nunique()
    num_chems_with_CasRN = data[data["CasRN"].notnull()]["CasRN"].nunique()
    # number with MESH and OMIM DiseaseID
    unique_diseases = data[["DiseaseID", "DiseaseName"]].drop_duplicates()
    num_diseases_with_MeshID = unique_diseases[unique_diseases["DiseaseID"].str.contains(
        "MESH")].shape[0]
    num_diseases_with_OmimID = unique_diseases[unique_diseases["DiseaseID"].str.contains(
        "OMIM")].shape[0]
    logger.info(
        f"Number of diseases with MESH DiseaseID: {num_diseases_with_MeshID}, with OMIM DiseaseID: {num_diseases_with_OmimID}")
    logger.info(
        f"Number of chemicals with ChemicalID: {num_chems_with_chemID}, with CasRN: {num_chems_with_CasRN}")
    num_with_pubmed = data[data["PubMedIDs"].notnull()].shape[0]
    logger.info(f"Number of entries with PubMedIDs: {num_with_pubmed}")


@click.command()
@click.option("--data_dir", default="./data", help="The directory containing the data.")
@click.option("--log_level", default="debug", help="The level at which to log.")
@click.option("--output_dir", default="./outputs", help="The directory to save the output.")
def main(data_dir, log_level, output_dir):
    # Set up logger
    logger = get_logger(log_level=log_level, logger_name="read_ctd_chemdis")
    ctd_data = load_ctd_data(data_dir)
    logger.info(f"CTD data shape: {ctd_data.shape}")
    log_relevant_info(ctd_data, logger, check_evidence=False)

    # get lines where DirectEvidence is not empty
    ctd_chemdis = ctd_data[ctd_data["DirectEvidence"].notnull()]
    logger.info(f"CTD_chemicals_diseases shape: {ctd_chemdis.shape}")
    log_relevant_info(ctd_chemdis, logger)

    ctd_chemdis_with_PubMedIds = ctd_chemdis[ctd_chemdis["PubMedIDs"].notnull(
    )]
    logger.info(
        f"CTD_chemicals_diseases with PubMedIds shape: {ctd_chemdis_with_PubMedIds.shape}")
    log_relevant_info(ctd_chemdis_with_PubMedIds, logger)

    ctd_chemdis.to_csv(os.path.join(
        output_dir, "CTD_chemicals_diseases_chemdis.csv"), index=False)


if __name__ == "__main__":
    main()
