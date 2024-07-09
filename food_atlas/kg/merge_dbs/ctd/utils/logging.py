"""
    Logging module for setting up loggers.
"""

import logging


def get_logger(logger_name="name", log_level="debug", output_file=None):
    """
    Get a logger with the given name that logs at the debug level.

    Args:
        logger_name: The name of the logger.
        log_level: The level at which to log.
        output_file: The file to output the logs to. If None, the logs are not output to a file.

    Returns:
        A logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, log_level.upper()))

    formatter = logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s:%(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if output_file is not None:
        file_handler = logging.FileHandler(output_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def log_data(logger, data, name):
    """
    Log the data loading information.

    Args:
        logger (logging.Logger): The logger to use.
        data (pd.DataFrame): The data to log.
        name (str): The name of the data.
    """
    logger.info(f"shape of {name}: {data.shape}")
    logger.info(f"head of {name}: {data.head()}")
