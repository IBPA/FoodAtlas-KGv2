"""
Uses functions for converting PMID to PMCID in fadisease/utils/data.py.
Creates a mapping from PMID to PMCID and saves it as a CSV file.
PMID file is from CTD, uses compare_factd_chem output

Usage:
    python -u -m fadisease.test_pmid_to_pmcid <email> --log_level <log_level> --output_dir <output_dir>

Author:
    Arielle Yoo - asmyoo@ucdavis.edu
"""

import os
import pandas as pd
import numpy as np
import logging
import click

from fadisease.utils.data import (
    create_pmid_to_pmcid_mapping,
    CTD_PUBMED_IDS_TO_PMCID_FILENAME,
    load_pmid_to_pmcid_mapping)
from fadisease.utils.logging import get_logger


@click.command()
@click.argument("email", type=str)
@click.option("--log_level", default="debug", help="The level at which to log.")
@click.option("--output_dir", default="./outputs", help="The directory to save the output.")
def main(
    email: str,
    log_level: str,
    output_dir: str
):
    logger = get_logger("test_pmid_to_pmcid", log_level)

    file_name = os.path.join(output_dir, CTD_PUBMED_IDS_TO_PMCID_FILENAME)
    if os.path.exists(file_name):
        df = load_pmid_to_pmcid_mapping(output_dir=output_dir)
        logger.info(f"Loaded the mapping from {file_name}.")
        logger.info(f"df shape: {df.shape}")
        logger.info(f"df head: {df.head()}")
    else:
        logger.info(f"Creating the mapping from PMID to PMCID.")
        df = create_pmid_to_pmcid_mapping(email, "test", output_dir, logger)
    return


if __name__ == "__main__":
    main()
