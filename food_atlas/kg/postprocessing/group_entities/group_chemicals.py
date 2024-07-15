import pandas as pd

from ._chemical_cdno import generate_chemical_groups_cdno
from ._chemical_chebi import generate_chemical_groups_chebi
from ... import KnowledgeGraph


if __name__ == '__main__':
    kg = KnowledgeGraph()

    groups_cdno = generate_chemical_groups_cdno(kg)
    groups_chebi = generate_chemical_groups_chebi(kg)

    groups = pd.concat([groups_chebi, groups_cdno], axis=1)
    groups.columns = ['chebi', 'cdno']

    groups.to_csv('outputs/kg/chemical_groups.tsv', sep='\t')
