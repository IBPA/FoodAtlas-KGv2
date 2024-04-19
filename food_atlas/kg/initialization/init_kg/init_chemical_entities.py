from ast import literal_eval

import pandas as pd

from ... import KnowledgeGraph
from ...entities._chemical import _create_chemical_entities_from_pubchem_compound
from ....tests import unit_test_kg


def initialize_chemical_entities_from_pubchem_compound(kg):
    records = pd.read_csv(
        "outputs/kg/initialization/_pubchem_compound.tsv",
        sep='\t',
        converters={'SynonymList': literal_eval},
    )
    records['iupac_name'] = records['IUPACName'].str.strip().str.lower()
    records['synonyms'] = records['SynonymList'].apply(
        lambda x: [s.strip().lower() for s in x]
    )

    _create_chemical_entities_from_pubchem_compound(kg.entities, records)


if __name__ == '__main__':
    kg = KnowledgeGraph(path_kg="outputs/kg/initialization")

    initialize_chemical_entities_from_pubchem_compound(kg)
    kg._disambiguate_synonyms()
    kg.save("outputs/kg/initialization")

    unit_test_kg.test_all(kg)
