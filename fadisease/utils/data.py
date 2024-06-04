"""
This module contains functions for loading data from the data directory.

Authors:
    Arielle Yoo - asmyoo@ucdavis.edu
"""
import os
import pandas as pd
import numpy as np
import logging
import requests
from bs4 import BeautifulSoup
from ast import literal_eval
import openpyxl
import re


CTD_CHEMDIS_DATA_FILENAME = "CTD_chemicals_diseases.csv"
CTD_CHEM_DATA_FILENAME = "CTD_chemicals.csv"
CTD_DISEASE_DATA_FILENAME = "CTD_diseases.csv"
PUBCHEM_TO_CTD_FILENAME = "PubChemToCTD.txt"
CTD_PUBMED_IDS_FILENAME = "CTD_pubmed_ids.txt"
CTD_PUBMED_IDS_TO_PMCID_FILENAME = "CTD_pubmed_ids_to_pmcid.csv"
# FA_DATA_FOLDER = "v2.0/zipped"
FA_DATA_FOLDER = "v2.0_foodon_fixed2/zipped"
FA_ENTITY_FILENAME = "entities.tsv"
FA_METADATA_FILENAME = "metadata_contains.tsv"
FA_TRIPLETS_FILENAME = "triplets.tsv"
FA_CHEM_LOOKUP_FILENAME = "lookup_table_chemical.tsv"
OFT_CHEMICALS_FILENAME = "SubstanceCharacterisation_KJ_2023.xlsx"
OFT_TOX_FILENAME = "ReferencePoints_KJ_2023.xlsx"
TVD_TRF_FILENAME = "toxval_all_res_toxval_v94_ToxRefDB.xlsx"
PUBCHEM_TO_CAS_FILENAME = "PubChemToCAS.txt"
FA_ID = "foodatlas_id"
FA_CHEM_NAME = "name"
ENTITY_TYPE = "entity_type"
EXTERNAL_IDS = "external_ids"
PUBCHEM_CID = "pubchem_cid"
MESH_IDS = "mesh_ids"
PUBMED_IDS = "pmid"
PMCID = "pmcid"
CTD_CHEMICAL_NAME = "ChemicalName"
CTD_CHEMICAL_ID = "ChemicalID"
CTD_DISEASE_ID = "DiseaseID"
CTD_DISEASE_EXTERNAL_ID = "mesh_ids"
CTD_DIRECTEVIDENCE = "DirectEvidence"
CTD_PUBMED_IDS = "PubMedIDs"
CTD_PUBMED_ID = "PubMedID"
OFT_CHEMICAL_NAME = "Substance"
TVD_CHEMICAL_NAME = "name"
CAS_ID = "CAS"
OFT_CAS_ID = "CASNumber"
TVD_CAS_ID = "casrn"
CTD_ALTID_MAPPING = {
    "DO": "diseaseontology_ids",
    "MESH": "mesh_ids",
    "OMIM": "omim_ids",
}
CTD_REVERSE_ALTID_MAPPING = {
    "diseaseontology_ids": "DO",
    "mesh_ids": "MESH",
    "omim_ids": "OMIM",
}
CTD_DIRECTEVIDENCE_MAPPING = {
    "marker/mechanism": "r3",
    "therapeutic": "r4",
}
OFT_COLUMN_MAPPING = {
    OFT_CHEMICAL_NAME: "chemical_name",
    OFT_CAS_ID: "cas_id",
    "Author": "specific_source",
    "Species": "species_common_name",
    "TestType": "test_type",
    "DurationDays": "duration",
    "Endpoint": "toxicity_endpoint",
    "qualifier": "qualifier",
    "value": "value",
    "unit": "unit"}
OFT_MAP_VALUES = {
    "Not reported": None,
    "unspecified": None,
    "not reported": None,
}
TVD_COLUMN_MAPPING = {
    TVD_CHEMICAL_NAME: "chemical_name",
    TVD_CAS_ID: "cas_id",
    "toxval_type": "toxicity_endpoint",
    "toxval_numeric_qualifier": "qualifier",
    "toxval_numeric": "value",
    "toxval_units": "unit",
    "study_duration_value": "duration",
    "study_duration_units": "duration_unit",
    "common_name": "species_common_name",
    "latin_name": "species",
    "sex": "species_sex",
    "lifestage": "species_lifestage",
    "study_type": "test_type",
    "exposure_route": "exposure_route",
    "exposure_method": "exposure_method",
    "critical_effect": "effect",
}
TOXICITY_METADATA_COLUMNS = [
    FA_ID,
    "species",
    "species_common_name",
    "species_sex",
    "species_lifestage",
    "chemical_name",
    "cas_id",
    "test_type",
    "duration",
    "duration_unit",
    "exposure_route",
    "exposure_method",
    "toxicity_endpoint",
    "qualifier",
    "value",
    "unit",
    "effect",
    "source",
    "specific_source",
]
TOXICITY_METADATA_LOWERCASE_COLUMNS = [
    "species",
    "species_common_name",
    "species_lifestage",
    "chemical_name",
    "duration_unit",
    "exposure_route",
    "exposure_method",
    "toxicity_endpoint",
    "source"]
CTD_COLUMNS_WITH_LISTS = ["OmimIDs", "PubMedIDs", "ParentIDs",
                          "TreeNumbers", "ParentTreeNumbers",
                          "Synonyms", "AltDiseaseIDs", "SlimMappings"]
PMID_PMCID_REQUEST_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/"


def change_content_to_list(
        df: pd.DataFrame,
        splitby: str = '|',
        columns_tosplit: list = CTD_COLUMNS_WITH_LISTS) -> pd.DataFrame:
    """
    Change the content of the columns in the DataFrame to lists.

    Args:
        df (pd.DataFrame): The DataFrame to change the content of.
        splitby (str): The string to split by. Default is '|'.
        columns_tosplit (str): The columns to change the content of.

    Returns:
        The DataFrame with the content of the columns changed to lists.
    """
    for column in columns_tosplit:
        if column in df.columns:
            df[column] = df[column].apply(
                lambda x: x.split(splitby) if pd.notnull(x) else [])
            # change to list of integers if possible
            df[column] = df[column].apply(
                lambda x: [int(i) if i.isdigit() else i for i in x])
    return df


def split_column_into_length(
        df: pd.DataFrame,
        column: str,
        splitby: str = '|',
        length: int = 2) -> pd.DataFrame:
    """
    Split the content of a column into a new columns based on the length.

    Args:
        df (pd.DataFrame): The DataFrame to split the content of.
        column (str): The column to split.
        splitby (str): The string to split by. Default is '|'.
        length (int): The length to split the content into. Default is 2.

    Returns:
        The DataFrame with the content of the column split into new columns.
    """
    def split_column(x, i):
        if pd.notnull(x) and len(x.split(splitby)) > i:
            return x.split(splitby)[i]
        elif pd.notnull(x):
            return x
        else:
            return None

    def split_last_column(x, i):
        if pd.notnull(x) and len(x.split(splitby)) > i:
            return splitby.join(x.split(splitby)[i:])
        elif pd.notnull(x):
            return x
        else:
            return None

    for i in range(length):
        # if last column, add the rest
        if i == length - 1:
            df[f"{column}_{i}"] = df[column].apply(
                lambda x: split_last_column(x, i))
        else:
            df[f"{column}_{i}"] = df[column].apply(
                lambda x: split_column(x, i))
    return df


def load_ctd_data(
        data_dir: str = "./data",
        type: str = "chemdis") -> pd.DataFrame:
    """
    Load the CTD data from the data directory.

    Args:
        data_dir (str): The directory containing the data.
        type (str): The type of CTD data to load. Default is "chemdis".

    Returns:
        A pandas DataFrame containing the CTD data.
    """
    file_path_dict = {
        "chemdis": CTD_CHEMDIS_DATA_FILENAME,
        "chem": CTD_CHEM_DATA_FILENAME,
        "disease": CTD_DISEASE_DATA_FILENAME
    }
    file_path = os.path.join(data_dir, file_path_dict[type])

    with open(file_path, 'r') as file:
        lines = file.readlines()
        fields_line_index = next(i for i, line in enumerate(
            lines) if line.strip() == '# Fields:')
        header_line_index = fields_line_index + 1
        header = lines[header_line_index].strip().replace("# ", "").split(',')

    df = pd.read_csv(file_path, comment='#',
                     skiprows=range(1, header_line_index),
                     names=header)
    # drop if all values are NaN
    df = df.dropna(how='all').reset_index(drop=True)
    df = change_content_to_list(df)

    return df


def load_foodatlas_data(
        data_dir: str = "./data",
        type: str = "entities") -> pd.DataFrame:
    """
    Load the food atlas data from the data directory.

    Args:
        data_dir (str): The directory containing the data.
        type (str): The type of food atlas data to load. Default is "entities".

    Returns:
        A pandas DataFrame containing the food atlas data.
    """
    file_path_dict = {
        "entities": os.path.join(FA_DATA_FOLDER, FA_ENTITY_FILENAME),
        "metadata": os.path.join(FA_DATA_FOLDER, FA_METADATA_FILENAME),
        "triplets": os.path.join(FA_DATA_FOLDER, FA_TRIPLETS_FILENAME),
        "chem_lookup": os.path.join(FA_DATA_FOLDER, FA_CHEM_LOOKUP_FILENAME),
        "folder_entities": FA_ENTITY_FILENAME,
        "folder_metadata": FA_METADATA_FILENAME,
        "folder_triplets": FA_TRIPLETS_FILENAME,
        "folder_chem_lookup": FA_CHEM_LOOKUP_FILENAME
    }
    file_path = os.path.join(data_dir, file_path_dict[type])
    df = pd.read_csv(file_path, sep='\t')
    # drop if all values are NaN
    df = df.dropna(how='all').reset_index(drop=True)

    if type == "entities":
        df[EXTERNAL_IDS] = df[EXTERNAL_IDS].apply(
            lambda x: literal_eval(x) if pd.notnull(x) else {})

    if type == "chem_lookup":
        df[FA_ID] = df[FA_ID].apply(lambda x: literal_eval(x))
        df[f"len_{FA_ID}"] = df[FA_ID].apply(lambda x: len(x))
        # check if length != 1
        if df[f"len_{FA_ID}"].sum() != df.shape[0]:
            raise ValueError(
                "Error: The length of the FoodAtlas IDs is not 1 for all rows.")
        df = df.drop(columns=[f"len_{FA_ID}"])
        df = df.explode(FA_ID).reset_index(drop=True)
        # check names are unique
        if df["name"].nunique() != df.shape[0]:
            # print("CHECKING")
            # print(df["name"].nunique())
            # print(df.shape[0])
            # get names that are not unique
            names = df["name"].value_counts()
            # print(names)
            names = names[names > 1]
            if names.shape[0] > 0:
                raise ValueError(
                    f"Error: The following FoodAtlas {type} names are not unique: {names}")

    return df


def adjust_FA_entities_data(
    data: pd.DataFrame,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Adjust the data from FoodAtlas to make it easier to compare with CTD data.

    Args:
        data (pd.DataFrame): The data to adjust.
        logger (logging.Logger): The logger to use.

    Returns:
        pd.DataFrame: The adjusted data.
    """
    # if string, convert using literal_eval
    if type(data[EXTERNAL_IDS][0]) == str:
        data[EXTERNAL_IDS] = data[EXTERNAL_IDS].apply(
            lambda x: literal_eval(x) if pd.notnull(x) else {})
    # get ids of chemicals for fa_chemicals from external_id
    check_ids = []
    for ids in data[EXTERNAL_IDS]:
        for id in ids:
            check_ids.append(id)
    check_ids = list(set(check_ids))
    check_ids.sort()
    if logger:
        logger.info(
            f"Number of unique external_ids in FoodAtlas: {len(check_ids)}")
        logger.info(f"IDs in FA: {check_ids}")

    # add columns for each id in check_ids
    for id in check_ids:
        data[id] = data[EXTERNAL_IDS].apply(
            lambda x: x[id] if id in x else None)
    if logger:
        logger.info(f"Columns added for each id in check_ids")

    # check_id = '_placeholder_to'
    for check_id in check_ids:
        # how many placeholders are there?
        num_with_id = data[check_id].notnull().sum()
        if logger:
            logger.info(
                f"Number of entities with {check_id} in FoodAtlas: {num_with_id}")

    return data, check_ids


def adjust_FA_chemicals_data(
    data: pd.DataFrame,
    logger: logging.Logger = None,
) -> pd.DataFrame:
    """
    Adjust the data from FoodAtlas to make it easier to compare with CTD data.

    Args:
        data (pd.DataFrame): The data to adjust.
        logger (logging.Logger): The logger to use.

    Returns:
        pd.DataFrame: The adjusted data.
    """
    # get only chemicals
    data = data[data[ENTITY_TYPE] == "chemical"].reset_index(drop=True)
    if logger:
        logger.info(
            f"Number of chemicals in FoodAtlas: {data['foodatlas_id'].nunique()}")

    return adjust_FA_entities_data(data, logger)


def load_pubchem_to_ctd(
        output_dir: str = "./output") -> pd.DataFrame:
    """
    Load the PubChem to CTD data from the output directory.

    Args:
        output_dir (str): The directory containing the output data.

    Returns:
        A pandas DataFrame containing the PubChem to CTD data.
    """
    file_path = os.path.join(output_dir, PUBCHEM_TO_CTD_FILENAME)
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File {file_path} does not exist. Please make sure to follow commented instructions in compare_factd_chem.")
    df = pd.read_csv(file_path, sep='\t', header=None)
    df.columns = [PUBCHEM_CID, CTD_CHEMICAL_ID]
    df = df.dropna(how='all').reset_index(drop=True)

    return df


def load_pubchem_to_ctd_mapping(
        output_dir: str = "./output",
        logger: logging.Logger = None) -> pd.DataFrame:
    """
    Load the mapping from PubChem to CTD data from the output directory.

    Args:
        output_dir (str): The directory containing the output data.
        logger (logging.Logger): The logger to use.

    Returns:
        A pandas DataFrame containing the mapping from PubChem to CTD data.
    """
    df = load_pubchem_to_ctd(output_dir)
    df_mapping = df.groupby(PUBCHEM_CID)[CTD_CHEMICAL_ID].apply(
        list).reset_index(name=CTD_CHEMICAL_ID)
    if logger:
        logger.info(
            f"Number of unique chemicals in PubChemToCTD: {df_mapping.shape[0]}")
    # drop where ChemicalID is not [NaN]
    df_mapping = df_mapping[~df_mapping[CTD_CHEMICAL_ID].apply(
        lambda x: x == [np.nan])].reset_index(drop=True)
    if logger:
        logger.info(
            f"Number of unique chemicals in PubChemToCTD after dropping NaN: {df_mapping.shape[0]}")

    return df_mapping


def create_pmid_to_pmcid_mapping(
        email: str,
        tool: str,
        output_dir: str = "./output",
        logger: logging.Logger = None) -> pd.DataFrame:
    """
    Create a mapping from PMID to PMCID.

    Args:
        email (str): The email to use for the request.
        tool (str): The tool to use for the request.
        output_dir (str): The directory containing the output data.
        logger (logging.Logger): The logger to use.

    Returns:
        A pandas DataFrame containing the mapping from PMID to PMCID.
    """
    file_path = os.path.join(output_dir, CTD_PUBMED_IDS_FILENAME)
    df = pd.read_csv(file_path, header=None)
    df = df.dropna(how='all').reset_index(drop=True)
    df.columns = [CTD_PUBMED_ID]

    middle_url = "?tool={}&email={}&ids=".format(tool, email)
    df_pmcid = pd.DataFrame(columns=[PUBMED_IDS, PMCID])
    dfs = []
    counter = 0
    # get PMCID for every 200 PubMedIDs
    for i in range(0, len(df), 200):
        counter += 1
        if logger:
            logger.info(f"Counter: {counter}")
        if i + 200 > len(df):
            test_ids = df[CTD_PUBMED_ID].tolist()[i:]
        else:
            test_ids = df[CTD_PUBMED_ID].tolist()[i:i+200]
        test_ids_str = ",".join([str(id) for id in test_ids])
        url = PMID_PMCID_REQUEST_URL + middle_url + test_ids_str
        response = requests.get(url)
        status_code = response.status_code
        if status_code != 200:
            if logger:
                logger.error(
                    f"Error: Status code {status_code} for request with url {url}")
        else:
            # if logger:
            #     logger.info(
            #         f"Status code {status_code} for request with url {url}")
            soup = BeautifulSoup(response.content, "html.parser")
            records = soup.find_all("record")
            pmid_list = [record.get("pmid") for record in records]
            pmcid_list = [record.get("pmcid") for record in records]
            pmid_pmcid_df = pd.DataFrame(
                {PUBMED_IDS: pmid_list, PMCID: pmcid_list})
            dfs.append(pmid_pmcid_df)
            # if logger:
            #     for i in range(len(pmid_list)):
            #         logger.info(
            #             f"PMCID for PubMedID {pmid_list[i]}: {pmcid_list[i]}")
    df_pmcid = pd.concat(dfs, ignore_index=True)
    df_pmcid = df_pmcid.sort_values(by=PUBMED_IDS).reset_index(drop=True)
    df_pmcid.to_csv(os.path.join(output_dir, CTD_PUBMED_IDS_TO_PMCID_FILENAME),
                    index=False)
    return df


def load_pmid_to_pmcid_mapping(
        output_dir: str = "./output") -> pd.DataFrame:
    """
    Load the mapping from PMID to PMCID data from the output directory.

    Args:
        output_dir: The directory containing the output data.

    Returns:
        A pandas DataFrame containing the mapping from PMID to PMCID data.
    """
    file_path = os.path.join(output_dir, CTD_PUBMED_IDS_TO_PMCID_FILENAME)
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File {file_path} does not exist. Please run make_pmid_to_pmcid first.")
    df = pd.read_csv(file_path)
    # drop nan rows
    df = df.dropna(how='all').reset_index(drop=True)
    # only get number from pmcid
    df[PMCID] = df[PMCID].apply(lambda x: x.split("PMC")[
                                1] if pd.notnull(x) else x)
    # drop where PMCID is NaN
    df = df[~df[PMCID].isnull()].reset_index(drop=True)
    # change to integer
    df[PMCID] = df[PMCID].apply(lambda x: int(x))

    return df


def load_oft_data(
        data_dir: str = "./data",
        type: str = "full") -> pd.DataFrame:
    """
    Load the OpenFoodTox data from the data directory.

    Args:
        data_dir: The directory containing the data.
        type: The type of OpenFoodTox data to load. Default is "chemicals".

    Returns:
        A pandas DataFrame containing the OpenFoodTox data.
    """
    file_path_dict = {
        "chemicals": OFT_CHEMICALS_FILENAME,
        "tox": OFT_TOX_FILENAME
    }
    if "full" in type:
        df_chem = load_oft_data(data_dir, "chemicals")
        df_tox = load_oft_data(data_dir, "tox")
        if "outer" in type:
            df = pd.merge(df_chem, df_tox, on=OFT_CHEMICAL_NAME, how="outer")
        else:
            df = pd.merge(df_chem, df_tox, on=OFT_CHEMICAL_NAME, how="inner")
        return df
    file_path = os.path.join(data_dir, file_path_dict[type])
    df = pd.read_excel(file_path)
    df = df.map(lambda x: openpyxl.utils.escape.unescape(
        x) if isinstance(x, str) else x)
    # drop if all values are NaN
    df = df.dropna(how='all').reset_index(drop=True)

    return df


def load_tvd_data(
        data_dir: str = "./data",
        type: str = "toxrefdb") -> pd.DataFrame:
    """
    Load the ToxRefDB data from the data directory.

    Args:
        data_dir (str): The directory containing the data.
        type (str): The type of ToxRefDB data to load. Default is "toxrefdb".

    Returns:
        A pandas DataFrame containing the ToxRefDB data.
    """
    file_path_dict = {
        "toxrefdb": TVD_TRF_FILENAME
    }
    file_path = os.path.join(data_dir, file_path_dict[type])
    df = pd.read_excel(file_path)
    # drop if all values are NaN
    df = df.dropna(how='all').reset_index(drop=True)

    return df


def load_pubchem_to_cas(
        output_dir: str = "./output",
        logger: logging.Logger = None) -> pd.DataFrame:
    """
    Load the PubChem to CAS data from the output directory.

    Args:
        output_dir (str): The directory containing the output data.
        logger (logging.Logger): The logger to use.

    Returns:
        A pandas DataFrame containing the PubChem to CAS data.
    """
    file_path = os.path.join(output_dir, PUBCHEM_TO_CAS_FILENAME)
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File {file_path} does not exist. Please make sure to follow commented instructions in compare_factd_chem.")
    df = pd.read_csv(file_path, sep='\t', header=None)
    df = df.dropna(how='all').reset_index(drop=True)
    df.columns = [PUBCHEM_CID, CAS_ID]
    # format CAS
    df = df.astype({CAS_ID: str})
    df[CAS_ID] = df[CAS_ID].apply(
        lambda x: "-".join([x[:-3], x[-3:-1], x[-1]]) if x != "nan" else None)
    # remove leading zeros from CAS
    df[CAS_ID] = df[CAS_ID].replace(r'^0+', '', regex=True)

    return df


def load_pubchem_to_cas_mapping(
        output_dir: str = "./output",
        logger: logging.Logger = None) -> pd.DataFrame:
    """
    Load the mapping from PubChem to CAS data from the output directory.

    Args:
        output_dir (str): The directory containing the output data.
        logger (logging.Logger): The logger to use.

    Returns:
        A pandas DataFrame containing the mapping from PubChem to CAS data.
    """
    df = load_pubchem_to_cas(output_dir, logger)
    df_mapping = df.groupby(PUBCHEM_CID)[CAS_ID].apply(
        list).reset_index(name=CAS_ID)
    if logger:
        logger.info(
            f"Number of unique chemicals in PubChemToCAS: {df_mapping.shape[0]}")
    # drop where CAS is not [None]
    df_mapping = df_mapping[~df_mapping[CAS_ID].apply(
        lambda x: x == [None])].reset_index(drop=True)
    if logger:
        logger.info(
            f"Number of unique chemicals in PubChemToCAS after dropping NaN: {df_mapping.shape[0]}")

    return df_mapping
