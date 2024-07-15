import pandas as pd

from ... import KnowledgeGraph
from ._food_foodon import generate_food_groups_foodon


if __name__ == '__main__':
    kg = KnowledgeGraph()

    groups_foodon = generate_food_groups_foodon(kg)

    groups = pd.concat([groups_foodon], axis=1)
    groups.columns = ['foodon']

    groups.to_csv('outputs/kg/food_groups.tsv', sep='\t')
