import pandas as pd


if __name__ == "__main__":
    metadata_new = pd.read_csv("outputs/kg/2025_10_10/_metadata_new.tsv", sep='\t')
    metadata_added = pd.read_csv("outputs/kg/metadata_contains.tsv", sep='\t')

    print(metadata_new)
    print(metadata_added)