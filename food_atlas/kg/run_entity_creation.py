# -*- coding: utf-8 -*-
"""A run script for creating new entities for names that are not present in the KG. This
script will attempt to retrieve the primary ID for the new entity if available.

Attributes:
    attr1 (type): Description of attr1.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

Todo:
    * TODOs

"""
import click

from . import KnowledgeGraph


def create_food_entities():
    pass


def create_chemical_entities():
    pass


@ click.command()
@ click.argument('path-input-dir', type=click.Path(exists=True))
def main(path_input_dir: str):
    # kg = KnowledgeGraph()
    # pass


if __name__ == '__main__':
    main()
