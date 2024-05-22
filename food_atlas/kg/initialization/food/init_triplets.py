from ast import literal_eval

import pandas as pd

from ._load_foodon import load_foodon


if __name__ == '__main__':
    foodon = load_foodon()
    foodon_food = foodon[foodon['is_food']]
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
        converters={'external_ids': literal_eval}
    ).set_index('foodatlas_id')

    map_foodon_to_faid = {}
    for faid, row in entities.iterrows():
        map_foodon_to_faid[row['external_ids']['foodon_id']] = faid

    foodon_ids = foodon_food.index.tolist()

    # Traverse the FoodOn hierarchy to generate is_a triplets
    triplets_isa_rows = []
    visited = set()
    curr_miid = 1
    for foodon_id in foodon_ids:
        queue = [foodon_id]
        while queue:
            current = queue.pop()
            if current in visited:
                continue

            visited.add(current)
            for parent in foodon_food.loc[current, 'parents']:
                if parent in foodon_food.index:
                    queue.append(parent)
                    triplets_isa_rows += [{
                        'foodatlas_id': None,
                        'head_id': map_foodon_to_faid[current],
                        'relationship_id': 'r2',
                        'tail_id': map_foodon_to_faid[parent],
                        'metadata_ids': [],
                    }]

    triplets_isa = pd.DataFrame(triplets_isa_rows)
    triplets_isa['foodatlas_id'] = [
        f"t{i}" for i in list(range(1, 1 + len(triplets_isa)))
    ]

    triplets_isa.to_csv("outputs/kg/triplets.tsv", sep='\t', index=False)
