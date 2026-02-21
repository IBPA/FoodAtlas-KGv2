import logging

from ._kg import KnowledgeGraph

logging.basicConfig(
    format="%(name)s %(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

__all__ = [
    "KnowledgeGraph",
]
