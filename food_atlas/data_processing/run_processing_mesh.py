import pandas as pd
import xmltodict
from tqdm import tqdm


def parse_mesh_desc():
    """Parse the MeSH Descriptor XML file and save it as a JSON file."""
    with open("data/MeSH/desc2024.xml") as f:
        desc_json = xmltodict.parse(f.read())

    mesh_desc_rows = []
    records = desc_json["DescriptorRecordSet"]["DescriptorRecord"]
    for record in tqdm(records):
        id_ = record["DescriptorUI"]
        name = record["DescriptorName"]["String"]
        tree_numbers = (
            record["TreeNumberList"]["TreeNumber"] if "TreeNumberList" in record else []
        )
        synonyms = []

        concepts = record["ConceptList"]["Concept"]
        if isinstance(concepts, list):
            pass
        elif isinstance(concepts, dict):
            concepts = [concepts]
        else:
            raise ValueError(f"Unknown type: {type(concepts)}")

        for concept in concepts:
            terms = concept["TermList"]["Term"]
            if isinstance(terms, list):
                pass
            elif isinstance(terms, dict):
                terms = [terms]
            else:
                raise ValueError(f"Unknown type: {type(terms)}")

            for term in terms:
                synonyms.append(term["String"])

        mesh_desc_rows += [
            {
                "id": id_,
                "name": name,
                "synonyms": synonyms,
                "tree_numbers": tree_numbers,
            }
        ]

    mesh_desc = pd.DataFrame(mesh_desc_rows)
    mesh_desc.to_json(
        "outputs/data_processing/mesh_desc_cleaned.json", orient="records", lines=True
    )


def parse_mesh_supp():
    """Parse the MeSH Supplementary XML file and save it as a JSON file."""
    with open("data/MeSH/supp2024.xml") as f:
        supp_json = xmltodict.parse(f.read())

    mesh_supp_rows = []
    records = supp_json["SupplementalRecordSet"]["SupplementalRecord"]
    for record in tqdm(records):
        id_ = record["SupplementalRecordUI"]
        name = record["SupplementalRecordName"]["String"]
        mapped_to = record["HeadingMappedToList"]["HeadingMappedTo"]
        if isinstance(mapped_to, list):
            pass
        elif isinstance(mapped_to, dict):
            mapped_to = [mapped_to]
        else:
            raise ValueError(f"Unknown type: {type(mapped_to)}")
        mapped_to = [x["DescriptorReferredTo"]["DescriptorUI"] for x in mapped_to]

        synonyms = []
        concepts = record["ConceptList"]["Concept"]
        if isinstance(concepts, list):
            pass
        elif isinstance(concepts, dict):
            concepts = [concepts]
        else:
            raise ValueError(f"Unknown type: {type(concepts)}")

        for concept in concepts:
            terms = concept["TermList"]["Term"]
            if isinstance(terms, list):
                pass
            elif isinstance(terms, dict):
                terms = [terms]
            else:
                raise ValueError(f"Unknown type: {type(terms)}")

            for term in terms:
                synonyms.append(term["String"])

        mesh_supp_rows += [
            {
                "id": id_,
                "name": name,
                "synonyms": synonyms,
                "mapped_to": mapped_to,
            }
        ]

    mesh_supp = pd.DataFrame(mesh_supp_rows)
    mesh_supp.to_json(
        "outputs/data_processing/mesh_supp_cleaned.json", orient="records", lines=True
    )
