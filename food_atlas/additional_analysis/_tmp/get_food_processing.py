from ast import literal_eval

import pandas as pd


if __name__ == '__main__':
    entities = pd.read_csv(
        "outputs/kg/entities.tsv", sep='\t', converters={'external_ids': literal_eval}
    ).query("entity_type == 'food'").set_index('foodatlas_id')
    entities['foodon_id'] = entities['external_ids'].apply(
        lambda x: x['foodon_id']
    )
    entities['count'] = 0

    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv", sep='\t', converters={'metadata_ids': literal_eval}
    ).query("relationship_id == 'r1'")

    def add_count(row):
        entities.loc[row['head_id'], 'count'] += len(row['metadata_ids'])

    triplets.apply(add_count, axis=1)

    entities = entities.sort_values('count', ascending=False)
    entities['parentheses'] = entities['common_name'].str.extract(r'\(([^)]+)\)')
    entities.to_csv("check.csv")
