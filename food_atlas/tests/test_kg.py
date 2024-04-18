import click

from .unit_test_kg import test_all
from ..kg import KnowledgeGraph


@click.command()
@click.argument(
    'path_kg',
    type=str,
)
def main(path_kg: str):
    kg = KnowledgeGraph(path_kg=path_kg)
    test_all(kg)
    print('All tests passed.')

if __name__ == '__main__':
    main()
