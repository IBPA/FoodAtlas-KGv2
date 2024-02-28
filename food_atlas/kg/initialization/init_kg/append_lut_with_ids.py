from collections import defaultdict

import pandas as pd

from ...utils import load_entities, load_lookup_tables


if __name__ == '__main__':
    entities = load_entities("outputs/kg/initialization/entities.tsv")
    lut_food, lut_chem = load_lookup_tables("outputs/kg/initialization")


    def _append_lut(row):
        # nonlocal lut_food_new, lut_chem_new

        if row['entity_type'] == 'food':
            for k, v in row['external_ids'].items():
                if k == 'ncbi_taxon_id':
                    lut_food_new[f"_FAEXTID:{k}_SEP_{v}"] += [row['foodatlas_id']]
                else:
                    for v_ in v:
                        lut_food_new[f"_FAEXTID:{k}_SEP_{v_}"] += [row['foodatlas_id']]
        elif row['entity_type'] == 'chemical':
            for k, v in row['external_ids'].items():
                if k == 'pubchem_cid':
                    lut_chem_new[f"_FAEXTID:{k}_SEP_{v}"] += [row['foodatlas_id']]
                else:
                    for v_ in v:
                        lut_chem_new[f"_FAEXTID:{k}_SEP_{v_}"] += [row['foodatlas_id']]

    lut_food_new = defaultdict(list)
    lut_chem_new = defaultdict(list)
    entities.apply(_append_lut, axis=1)

    for k, v in lut_food_new.items():
        if k in lut_food:
            raise ValueError(f"Key {k} already exists in lut_food")
        lut_food[k] = v

    for k, v in lut_chem_new.items():
        if k in lut_chem:
            raise ValueError(f"Key {k} already exists in lut_chem")
        lut_chem[k] = v

    pd.DataFrame(lut_food.items(), columns=['name', 'foodatlas_id']).to_csv(
        "outputs/kg/initialization/lookup_table_food.tsv", sep='\t', index=False
    )

    pd.DataFrame(lut_chem.items(), columns=['name', 'foodatlas_id']).to_csv(
        "outputs/kg/initialization/lookup_table_chemical.tsv", sep='\t', index=False
    )
