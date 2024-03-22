from ast import literal_eval

import pandas as pd


if __name__ == '__main__':
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep='\t',
        converters={'metadata_ids': literal_eval},
    )
    triplets['metadata_ids'] = triplets['metadata_ids'].apply(set)
    triplets['metadata_ids'] = triplets['metadata_ids'].apply(list)

    triplets.to_csv("outputs/kg/triplets.tsv", sep='\t', index=False)
    print(triplets)
