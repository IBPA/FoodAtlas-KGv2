"""
Compare chemicals in CTD with chemicals in FoodAtlas.
Requires read_ctd_chemdis.py output and utils/data.py.

Usage:
    python -u -m fadisease.compare_factd_chem --data_dir=<data_dir> --log_level=<log_level> --output_dir=<output_dir>

Authors:
    Arielle Yoo - asmyoo@ucdavis.edu
"""
import os
import pandas as pd
import click

from fadisease.utils.data import (
    load_foodatlas_data,
    load_ctd_data,
    adjust_FA_chemicals_data,
    load_pubchem_to_ctd,
    load_pubchem_to_ctd_mapping
)
from fadisease.utils.data import (
    PUBCHEM_TO_CTD_FILENAME,
    FA_ID,
    PUBCHEM_CID,
    CTD_DIRECTEVIDENCE,
    CTD_CHEMICAL_ID,
    CTD_CHEMICAL_NAME
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
    logger = get_logger(log_level=log_level, logger_name="compare_factd_chem")

    # Load the data
    fa_entities = load_foodatlas_data(data_dir)
    fa_chemicals, check_ids = adjust_FA_chemicals_data(fa_entities, logger)
    ctd_data = pd.read_csv(os.path.join(
        output_dir, "CTD_chemicals_diseases_chemdis.csv"))
    ctd_data_uniquechem = ctd_data.drop_duplicates(subset=[CTD_CHEMICAL_ID])
    logger.info(
        f"Number of unique chemicals in CTD triples: {ctd_data_uniquechem.shape[0]}")

    # get chemicals from fa_chemicals with pubchem_cid
    fa_chemicals_cid = fa_chemicals[fa_chemicals[PUBCHEM_CID].notnull()].reset_index(
        drop=True)
    # get unique pubchem_cid
    fa_chemicals_cid_unique = fa_chemicals_cid.drop_duplicates(
        subset=[PUBCHEM_CID]).reset_index(drop=True)
    logger.info(
        f"Number of unique chemicals in FoodAtlas with pubchem_cid: {fa_chemicals_cid_unique.shape[0]}")

    if not os.path.exists(os.path.join(output_dir, PUBCHEM_TO_CTD_FILENAME)):
        # Put FA_chemicals_pubchem_cid.csv into PubChem Identifier Exchange Service (https://pubchem.ncbi.nlm.nih.gov/idexchange/idexchange.cgi)
        # Select the following:
        # * Input ID List: CIDs
        # * Choose File: FA_chemicals_pubchem_cid.csv
        # * Operator Type: Same CID
        # * Output IDs: Registry IDs, Comparative Toxicogenomics Database (CTD) Chemicals
        # * Output Methods: Two Column file
        # * Compression: No compression
        # After downloading the file into the output directory, rename it to PUBCHEM_TO_CTD_FILENAME
        # Repeat this process but change
        # * Output IDs: Registry IDs, ChemIDPlus
        # After downloading the file into the output directory, rename it to PUBCHEM_TO_CAS_FILENAME
        # Rerun this script
        fa_chemicals_cid_unique.to_csv(os.path.join(
            output_dir, "FA_chemicals_pubchem.csv"), index=False)
        # change pubchem_cid to int
        fa_chemicals_cid_unique = fa_chemicals_cid_unique.astype(
            {PUBCHEM_CID: int})
        fa_chemicals_cid_unique[PUBCHEM_CID].to_csv(os.path.join(
            output_dir, "FA_chemicals_pubchem_cid.csv"), index=False, header=False)
        # get all PubMedIDs to convert to pmcid in another script
        ctd_data = load_ctd_data(data_dir)
        # get all PubMedIDs in ctd_data
        pubmed_ids = []
        for pubmedid_list in ctd_data["PubMedIDs"]:
            if len(pubmedid_list) > 0:
                pubmed_ids.extend(pubmedid_list)
        # get unique PubMedIDs
        pubmed_ids_unique = list(set(pubmed_ids))
        pubmed_ids_unique.sort()
        # output to file
        with open(os.path.join(output_dir, "CTD_pubmed_ids.txt"), "w") as f:
            for pubmed_id in pubmed_ids_unique:
                f.write(f"{pubmed_id}\n")
        return

    # read PubChemToCTD.txt
    pubchem_to_ctd = load_pubchem_to_ctd(output_dir)
    logger.info(
        f"Number of chemicals in PubChemToCTD: {pubchem_to_ctd.shape[0]}")
    # check duplicate pubchem_cid
    pubchem_to_ctd_unique = load_pubchem_to_ctd_mapping(output_dir, logger)
    pubchem_to_ctd_unique[f"len_{CTD_CHEMICAL_ID}"] = pubchem_to_ctd_unique[CTD_CHEMICAL_ID].apply(
        lambda x: len(x))
    logger.info(
        f"Number of {CTD_CHEMICAL_ID} in PubChemToCTD: {pubchem_to_ctd_unique[f'len_{CTD_CHEMICAL_ID}'].sum()}")
    # check for overlap between PubChemToCTD and CTD
    pubchem_to_ctd_unique["overlap"] = pubchem_to_ctd_unique[CTD_CHEMICAL_ID].apply(
        lambda x: len(set(x) & set(ctd_data_uniquechem[CTD_CHEMICAL_ID])))
    # sum where overlap > 0
    logger.info(
        f"Number of unique chemicals in PubChemToCTD with overlap: {pubchem_to_ctd_unique[pubchem_to_ctd_unique['overlap'] > 0].shape[0]}")
    logger.info(
        f"Number of overlap between PubChemToCTD and CTD: {pubchem_to_ctd_unique['overlap'].sum()}")
    # values
    logger.info(
        f"len_{CTD_CHEMICAL_ID} values: {pubchem_to_ctd_unique[f'len_{CTD_CHEMICAL_ID}'].value_counts().sort_index()}"
    )
    logger.info(
        f"Overlap values: {pubchem_to_ctd_unique['overlap'].value_counts().sort_index()}")
    pubchem_to_ctd_unique.to_csv(os.path.join(
        output_dir, "PubChemToCTD_uniquePubChemID.csv"), index=False)

    ctd_chemdis = load_ctd_data(data_dir)
    ctd_chemdis = ctd_chemdis[ctd_chemdis[CTD_DIRECTEVIDENCE].notnull()].reset_index(
        drop=True)
    # drop where ChemicalID is not in PubChemToCTD
    meshids_in_pubchem_to_ctd = set(
        pubchem_to_ctd_unique[CTD_CHEMICAL_ID].explode())
    ctd_chemdis = ctd_chemdis[ctd_chemdis[CTD_CHEMICAL_ID].isin(
        meshids_in_pubchem_to_ctd)].reset_index(drop=True)
    logger.info(
        f"Number of triples in CTD chemdis by mesh that are in PubChemToCTD: {ctd_chemdis.shape[0]}")
    # mapper from ChemicalID to PubChemID
    meshid_to_pubchemid = pubchem_to_ctd_unique.explode(CTD_CHEMICAL_ID)
    ctd_chemdis[PUBCHEM_CID] = ctd_chemdis[CTD_CHEMICAL_ID].map(
        meshid_to_pubchemid.set_index(CTD_CHEMICAL_ID)[PUBCHEM_CID])
    # group by pubchem_cid to count number of triples per pubchem_cid
    ctd_chemdis_grouped = ctd_chemdis.groupby(PUBCHEM_CID).size().reset_index(
        name="count")
    logger.info(
        f"Triple count per {PUBCHEM_CID}: {ctd_chemdis_grouped['count'].value_counts().sort_index()}")
    # group by ChemicalID to count number of triples per ChemicalID
    ctd_chemdis_grouped_mesh = ctd_chemdis.groupby(CTD_CHEMICAL_ID).size().reset_index(
        name="count")
    logger.info(
        f"Triple count per {CTD_CHEMICAL_ID}: {ctd_chemdis_grouped_mesh['count'].value_counts().sort_index()}")

    # checking what happens if map with substring
    fa_lookup = load_foodatlas_data(data_dir, "chem_lookup")
    logger.info(f"fa_lookup: {fa_lookup.head()}")
    fa_lookup = fa_lookup.set_index("name")[FA_ID].to_dict()
    # check where FA_ID is unique and overlap with PubChemToCTD is 0
    ctd_chemdis = load_ctd_data(data_dir)
    ctd_chemdis = ctd_chemdis[ctd_chemdis[CTD_DIRECTEVIDENCE].notnull()].reset_index(
        drop=True)
    ctd_chemdis[PUBCHEM_CID] = ctd_chemdis[CTD_CHEMICAL_ID].map(
        meshid_to_pubchemid.set_index(CTD_CHEMICAL_ID)[PUBCHEM_CID])
    # check if chemical name in fa_lookup
    ctd_chemdis[f"lower_{CTD_CHEMICAL_NAME}"] = ctd_chemdis[CTD_CHEMICAL_NAME].str.lower()
    ctd_chemdis[FA_ID] = ctd_chemdis[f"lower_{CTD_CHEMICAL_NAME}"].map(
        fa_lookup)
    ctd_chemdis_unique = ctd_chemdis.drop_duplicates(subset=[CTD_CHEMICAL_ID])
    logger.info(
        f"Number of unique chemicals in CTD triples by {CTD_CHEMICAL_ID}: {ctd_chemdis_unique.shape[0]}")
    logger.info(
        f"Mapping by ctd chemicals by name, number of unique FA_ID chem: {ctd_chemdis[FA_ID].nunique()}")
    logger.info(
        f"Number of chemicals in CTD chemdis that are in FoodAtlas with PubChemID mapping: {ctd_chemdis_unique[ctd_chemdis_unique[PUBCHEM_CID].notnull()].shape[0]}")
    logger.info(
        f"Number of unique chemicals in CTD chemdis that are in FoodAtlas based on unique PubChemID mapping: {ctd_chemdis_unique[PUBCHEM_CID].nunique()}")
    ctd_chemdis_unique.to_csv(os.path.join(
        output_dir, "CTD_chemicals_diseases_chemdis_unique.csv"), index=False)
    # drop where FA_ID is null
    ctd_chemdis_unique_hasFA_ID = ctd_chemdis_unique[ctd_chemdis_unique[FA_ID].notnull(
    )]
    logger.info(
        f"Number of unique chemicals (checking FA_ID) in CTD chemdis that are in FoodAtlas by name: {ctd_chemdis_unique_hasFA_ID[FA_ID].nunique()}")
    # drop where pubchem_cid is null
    ctd_chemdis_unique_hasFA_ID_hasPubChemID = ctd_chemdis_unique_hasFA_ID[
        ctd_chemdis_unique_hasFA_ID[PUBCHEM_CID].notnull()]
    ctd_chemdis_unique_hasFA_ID_noPubChemID = ctd_chemdis_unique_hasFA_ID[ctd_chemdis_unique_hasFA_ID[PUBCHEM_CID].isnull(
    )]
    logger.info(
        f"Number of unique chemicals in CTD chemdis (checking FA_ID) that are in FoodAtlas by name with PubChemID mapping: {ctd_chemdis_unique_hasFA_ID_hasPubChemID[FA_ID].nunique()}")
    logger.info(
        f"Number of unique chemicals in CTD chemdis (checking FA_ID) that are in FoodAtlas by name without PubChemID mapping: {ctd_chemdis_unique_hasFA_ID_noPubChemID[FA_ID].nunique()}")
    overlapping_FA_ID = set(ctd_chemdis_unique_hasFA_ID_hasPubChemID[FA_ID]) & set(
        ctd_chemdis_unique_hasFA_ID_noPubChemID[FA_ID])
    logger.info(
        f"overlap between FA_IDs with PubChemID mapping and without PubChemID mapping in CTD: {len(overlapping_FA_ID)}")
    # output to file
    overlapping_FA_ID = list(overlapping_FA_ID)
    overlapping_FA_ID.sort()
    with open(os.path.join(output_dir, "overlap_FA_ID.txt"), "w") as f:
        for fa_id in overlapping_FA_ID:
            f.write(f"{fa_id}\n")
    overlapping_FA_ID = set(ctd_chemdis_unique_hasFA_ID_hasPubChemID[FA_ID]) & set(
        fa_chemicals_cid_unique[FA_ID])
    logger.info(
        f"overlap between FA_IDs with PubChemID mapping in CTD and FA_IDs with PubChem -> mesh value: {len(overlapping_FA_ID)}")
    overlapping_FA_ID = set(ctd_chemdis_unique_hasFA_ID_noPubChemID[FA_ID]) & set(
        fa_chemicals_cid_unique[FA_ID])
    logger.info(
        f"overlap between FA_IDs without PubChemID mapping in CTD and FA_IDs with PubChem -> mesh value: {len(overlapping_FA_ID)}")
    logger.info(
        f"Number of triples in CTD chemdis by FA_ID that are in FoodAtlas by name: {ctd_chemdis[FA_ID].notnull().sum()}")
    ctd_chemdis_grouped_FA_ID = ctd_chemdis.groupby(FA_ID).size().reset_index(
        name="count")
    logger.info(
        f"Triple count per {FA_ID} from mapping chemicals by name: {ctd_chemdis_grouped_FA_ID['count'].value_counts().sort_index()}")


if __name__ == "__main__":
    main()
