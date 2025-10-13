import pandas as pd

if __name__ == "__main__":
    # The SID-Map file is too large to load frequentlt. We will ignore everything that
    # is not from ChEBI and convert it to parquet.
    sids = pd.read_csv(
        "data/PubChem/SID-Map",
        sep="\t",
        header=None,
        names=["SID", "source", "registry_id", "cid"],
    )

    sids.query("source == 'ChEBI'").to_parquet(
        "outputs/data_processing/pubchem-sid-map-small.parquet"
    )
