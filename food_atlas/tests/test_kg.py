import logging

import click

from .unit_test_kg import test_all
from ..kg import KnowledgeGraph

logger = logging.getLogger(__name__)


@click.command()
@click.argument(
    'path_kg',
    type=str,
)
def main(path_kg: str):
    logger.info('Running unit tests for the knowledge graph...')

    kg = KnowledgeGraph(path_kg=path_kg)
    test_all(kg)

    logger.info('All tests passed.')

if __name__ == '__main__':
    main()
