import re
from ast import literal_eval

import pandas as pd

from .create_factd_data import create_disease_entities, get_max_fa_id
from .utils.data import (
    CTD_DIRECTEVIDENCE,
    CTD_DIRECTEVIDENCE_MAPPING,
    CTD_PUBMED_IDS,
    FA_ID,
    PMCID,
    PUBMED_IDS,
    load_ctd_data,
    load_pmid_to_pmcid_mapping,
)


def create_disease_triplets_metadata(
    fa_triplets: pd.DataFrame,
    ctd_chemdis: pd.DataFrame,
    fa_entities: pd.DataFrame,
    fa_max_triplet_id: int,
) -> tuple:
    """TODO.
    Create disease triplets and metadata in FoodAtlas.

    Args:
        fa_triplets (pd.DataFrame): The FoodAtlas triplets.
        fa_metadata (pd.DataFrame): The FoodAtlas metadata.
        ctd_chemdis (pd.DataFrame): The filtered CTD chemical-disease associations.
        fa_entities (pd.DataFrame): The FoodAtlas entities that have been updated with
            disease entities.
        fa_max_triplet_id (int): The maximum triplet id in FoodAtlas.
        fa_chem_lookup (dict): The lookup from FoodAtlas chemical name to entity id.
        logger (logging.Logger): The logger to use.
        chem_id_pubchem_name_to_fa_id_not_match_file (str): The file to save the
            entities where converting using PubChem CID and chemical name to FoodAtlas
            entity id did not match.

    Returns:
        tuple (pd.DataFrame, pd.DataFrame): The updated FoodAtlas triplets and metadata.
    """
    # Mapping from CTD to FoodAtlas.
    fa_entities_chemicals = fa_entities.query("entity_type == 'chemical'")
    ctd2fa_chem = {}
    for _, row in fa_entities_chemicals.iterrows():
        for mesh_id in row["external_ids"].get("mesh", []):
            if mesh_id not in ctd2fa_chem:
                ctd2fa_chem[mesh_id] = []
            ctd2fa_chem[mesh_id] += [row[FA_ID]]

    fa_entities_diseases = fa_entities.query("entity_type == 'disease'")
    ctd2fa_disease = {}
    for _, row in fa_entities_diseases.iterrows():
        # MeSH and OMIM are enough to uniquely identify disease entities.
        if "mesh" in row["external_ids"]:
            for mesh_id in row["external_ids"]["mesh"]:
                if f"MESH:{mesh_id}" in ctd2fa_disease:
                    raise ValueError(f"mesh_id {mesh_id} already in ctd2fa_disease")
                ctd2fa_disease[f"MESH:{mesh_id}"] = [row[FA_ID]]
        elif "omim" in row["external_ids"]:
            for omim_id in row["external_ids"]["omim"]:
                if f"OMIM:{omim_id}" in ctd2fa_disease:
                    raise ValueError(f"omim_id {omim_id} already in ctd2fa_disease")
                ctd2fa_disease[f"OMIM:{omim_id}"] = [row[FA_ID]]

    ctd_chemdis["head_id"] = ctd_chemdis["ChemicalID"].apply(
        lambda x: ctd2fa_chem.get(x, [])
    )
    ctd_chemdis["tail_id"] = ctd_chemdis["DiseaseID"].apply(
        lambda x: ctd2fa_disease.get(x, [])
    )
    print(f"# rows with matching IDs: {len(ctd_chemdis)}")

    # # create triplets and metadata
    # ctd_chemdis["head_id"] = ctd_chemdis[CTD_CHEMICAL_ID].apply(
    #     lambda x: mesh_ids_to_fa_id_chemicals[x] if x in mesh_ids_to_fa_id_chemicals else None)
    # ctd_chemdis["head_id_name"] = ctd_chemdis[CTD_CHEMICAL_NAME].str.lower().map(
    #     fa_chem_lookup)
    # if logger:
    #     logger.info(
    #         f"Number of unique head_id: {ctd_chemdis['head_id'].nunique()}")
    #     logger.info(
    #         f"Number of unique head_id_name: {ctd_chemdis['head_id_name'].nunique()}")
    #     # check if head_id and head_id_name are the same for all when neither are None
    #     logger.info(
    #         f"Number of head_id and head_id_name that are not None: {ctd_chemdis[(ctd_chemdis['head_id'].notnull()) & (ctd_chemdis['head_id_name'].notnull())].shape[0]}")
    #     logger.info(
    #         f"Number of head_id and head_id_name that are not the same: {ctd_chemdis[(ctd_chemdis['head_id'].notnull()) & (ctd_chemdis['head_id_name'].notnull()) & (ctd_chemdis['head_id'] != ctd_chemdis['head_id_name'])].shape[0]}")
    # if chem_id_pubchem_name_to_fa_id_not_match_file:
    #     # save entities where head_id and head_id_name do not match
    #     ctd_chemdis_not_match = ctd_chemdis[(ctd_chemdis['head_id'].notnull()) & (
    #         ctd_chemdis['head_id_name'].notnull()) & (ctd_chemdis['head_id'] != ctd_chemdis['head_id_name'])]
    #     ctd_chemdis_not_match.to_csv(chem_id_pubchem_name_to_fa_id_not_match_file,
    #                                  index=False, sep='\t')
    # ctd_chemdis["head_id"] = ctd_chemdis.apply(
    #     lambda x: x["head_id"] if pd.notnull(x["head_id"]) else x["head_id_name"], axis=1)

    ctd_chemdis = ctd_chemdis.explode("head_id").explode("tail_id")
    print(f"# triplets: {len(ctd_chemdis)}")
    # ctd_chemdis["head_id"] = ctd_chemdis["head_id"].combine_first(
    #     ctd_chemdis["head_id_name"])
    # # ctd_chemdis["head_id"] = ctd_chemdis["head_id_name"].combine_first(
    # #     ctd_chemdis["head_id"])
    ctd_chemdis["relationship_id"] = ctd_chemdis[CTD_DIRECTEVIDENCE].apply(
        lambda x: CTD_DIRECTEVIDENCE_MAPPING[x]
    )
    # ctd_chemdis["tail_id"] = ctd_chemdis[CTD_DISEASE_ID].apply(
    #     lambda x: mesh_ids_to_fa_id_diseases[x])
    # if logger:
    #     logger.info(
    #         f"ctd_chemdis unique chemicals: {ctd_chemdis['head_id'].nunique()}")
    #     logger.info(
    #         f"ctd_chemdis relationship counts: {ctd_chemdis['relationship_id'].value_counts()}")
    #     logger.info(
    #         f"ctd_chemdis unique diseases: {ctd_chemdis['tail_id'].nunique()}")
    #     logger.info(
    #         f"ctd_chemdis number of triplets: {ctd_chemdis.shape[0]}")
    #     logger.info(
    #         f"ctd_chemdis number of unique triplets: {ctd_chemdis[['head_id', 'relationship_id', 'tail_id']].drop_duplicates().shape[0]}")
    #     # check number of triplets by head_id
    #     logger.info(
    #         f"ctd_chemdis number of triplets by head_id:\n{ctd_chemdis.groupby('head_id').size().value_counts().sort_index()}")
    #     # check number of triplets by CTD_CHEMICAL_ID
    #     logger.info(
    #         f"ctd_chemdis number of triplets by CTD_CHEMICAL_ID:\n{ctd_chemdis.groupby(CTD_CHEMICAL_ID).size().value_counts().sort_index()}")
    # explode CTD_PUBMED_IDS so that each pubmed_id is in a separate row
    ctd_chemdis = ctd_chemdis.explode(CTD_PUBMED_IDS).reset_index(drop=True)
    print(f"# metadata rows: {len(ctd_chemdis)}")

    ctd_chemdis["metadata_ids"] = ctd_chemdis.apply(
        lambda x: [f"md{x.name + 1}"], axis=1
    )

    # create metadata for triplets
    ctd_chemdis_metadata = pd.DataFrame()
    ctd_chemdis_metadata[FA_ID] = ctd_chemdis["metadata_ids"].apply(lambda x: x[0])
    ctd_chemdis_metadata["source"] = "ctd"

    pubmed_to_pmcid = load_pmid_to_pmcid_mapping(output_dir="outputs/data_processing")
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
    ctd_chemdis_metadata["entity_linking_method"] = "id_matching"
    ctd_chemdis_metadata["_chemical_name"] = ctd_chemdis["ChemicalID"].apply(
        lambda x: f"MESH:{x}"
    )
    ctd_chemdis_metadata["_disease_name"] = ctd_chemdis["DiseaseID"]

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
    # if logger:
    #     # should be equal to number of unique triplets
    #     # logger.info(
    #     #     f"ctd_chemdis number of triplets after grouping: {ctd_chemdis.shape[0]}")
    #     # logger.info(
    #     #     f"ctd_chemdis number of unique triplets after grouping: {ctd_chemdis[['head_id', 'relationship_id', 'tail_id']].drop_duplicates().shape[0]}")
    #     logger.info(
    #         f"ctd_chemdis relationship counts after grouping: {ctd_chemdis['relationship_id'].value_counts()}")
    #     ctd_chemdis["metadata_ids_len"] = ctd_chemdis["metadata_ids"].apply(
    #         lambda x: len(x))
    #     logger.info(
    #         f"ctd_chemdis metadata_ids_len: {ctd_chemdis['metadata_ids_len'].value_counts()}")
    # add triplets and metadata to fa_triplets and fa_metadata
    fa_triplets = pd.concat([fa_triplets, ctd_chemdis], join="inner", ignore_index=True)

    return fa_triplets, ctd_chemdis_metadata


if __name__ == "__main__":
    # Prepare FoodAtlas entities.
    fa_entities = pd.read_csv("outputs/kg/entities.tsv", sep="\t")
    fa_entities["external_ids"] = fa_entities["external_ids"].apply(
        lambda x: literal_eval(x) if pd.notnull(x) else {}
    )
    # Prepare CTD data.
    ctd_diseases = load_ctd_data(data_dir="data/CTD", type="disease")
    ctd_chemdis = pd.read_csv(
        "outputs/data_processing/ctd_chemdis_cleaned.tsv",
        sep="\t",
        converters={
            "PubMedIDs": literal_eval,
        },
    )

    fa_entities = create_disease_entities(
        fa_entities,
        ctd_diseases,
        ctd_chemdis,
        get_max_fa_id(fa_entities),
    )
    # fa_entities = pd.read_csv("outputs/kg/entities.tsv", sep='\t', converters={'external_ids': literal_eval})

    # update triplets and create new metadata
    fa_triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep="\t",
    )
    fa_chem_lookup = pd.read_csv(
        "outputs/kg/lookup_table_chemical.tsv",
        sep="\t",
    )
    fa_max_triplet_id = get_max_fa_id(fa_triplets)
    fa_triplets, fa_metadata = create_disease_triplets_metadata(
        fa_triplets, ctd_chemdis, fa_entities, fa_max_triplet_id
    )

    fa_entities.to_csv(
        "outputs/kg/entities.tsv",
        sep="\t",
        index=False,
    )
    fa_triplets.to_csv(
        "outputs/kg/triplets.tsv",
        sep="\t",
        index=False,
    )
    fa_metadata.to_csv(
        "outputs/kg/metadata_diseases.tsv",
        sep="\t",
        index=False,
    )
