from ast import literal_eval

import pandas as pd


if __name__ == '__main__':
    # Get the number of entities that appeared in metadata.
    metadata = pd.read_csv(
        "outputs/kg/metadata_contains.tsv",
        sep='\t',
    ).set_index('foodatlas_id')
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep='\t',
        converters={'metadata_ids': literal_eval},
    )
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
    ).set_index('foodatlas_id')

    def add_n_metadata(row):
        n = len(row['metadata_ids'])
        entities.loc[row['head_id'], 'count_all'] += n
        entities.loc[row['tail_id'], 'count_all'] += n

        for mid in row['metadata_ids']:
            if metadata.loc[mid, 'source'] == 'lit2kg:gpt-4':
                entities.loc[row['head_id'], 'count_gpt'] += 1
                entities.loc[row['tail_id'], 'count_gpt'] += 1

    entities['count_all'] = 0
    entities['count_gpt'] = 0
    triplets.apply(
        add_n_metadata,
        axis=1,
    )
    entities.to_csv("entities_with_count.tsv", sep='\t')
