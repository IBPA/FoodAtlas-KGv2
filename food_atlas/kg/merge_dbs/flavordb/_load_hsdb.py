import json


def load_hsdb():
    """ """
    hsdb_odor = json.load(
        open(
            "data/HSDB/"
            "PubChemAnnotations_Hazardous Substances Data Bank (HSDB)_heading=Odor.json",
            "r",
        )
    )
    hsdb_taste = json.load(
        open(
            "data/HSDB/"
            "PubChemAnnotations_Hazardous Substances Data Bank (HSDB)_heading=Taste.json",
            "r",
        )
    )

    def map_cid_to_hsdb_flavor(hsdb, mapper):
        for annot in hsdb["Annotations"]["Annotation"]:
            if "LinkedRecords" not in annot:
                continue

            for cid in annot["LinkedRecords"]["CID"]:
                if cid not in mapper:
                    mapper[cid] = []

                for data in annot["Data"]:
                    value = data["Value"]["StringWithMarkup"]
                    assert len(value) == 1

                    result = {}
                    result["value"] = value[0]["String"]
                    result["hsdb_url"] = annot["URL"]
                    mapper[cid].append(result)

    # Mapping CID to flavor record.
    cid2odor = {}
    cid2taste = {}
    map_cid_to_hsdb_flavor(hsdb_odor, cid2odor)
    map_cid_to_hsdb_flavor(hsdb_taste, cid2taste)

    return cid2odor, cid2taste
