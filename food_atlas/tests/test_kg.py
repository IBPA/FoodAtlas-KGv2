from .unit_test_kg import test_all
from ..kg import KnowledgeGraph


if __name__ == '__main__':
    kg = KnowledgeGraph(path_kg="outputs/kg/20240329")
    test_all(kg)
    print('All tests passed.')
