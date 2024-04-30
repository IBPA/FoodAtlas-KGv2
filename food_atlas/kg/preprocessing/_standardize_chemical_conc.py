import re

import numpy as np
import pandas as pd
from tqdm import tqdm

from .constants._units import UNITS
from .constants._units import UNITS_MASS, UNITS_VOLUME, UNITS_MOLE, UNITS_ENERGY

tqdm.pandas()

APPROXIMATES = [
    '<', '>', '≈', '~', '∼',
]

RANGES = [
    u'\u002d', u'\u2010', u'\u2011', u'\u2012', u'\u2013', u'\u2014', u'\u2015',
    u'\u2212',
    '~', '∼', 'to',
]

WEIGHT_TYPES = [
    # Fresh weight.
    'freshweight',
    'offw',
    'fw',

    # Dry weight.
    'dryweight',
    'dw',
    'dm',
]

PHENOLIC_UNITS = [
    'gae',
    'ce',
    'qe',
    're',
]


def _get_regex() -> str:
    """A helper that returns the long regex string.

    Returns:
        str: The long regex string.

    """
    re_value = r"(\d+(\.\d+)?(±\d+(\.\d+)?)?)"
    re_weight =   f"({'|'.join(WEIGHT_TYPES)})"
    # re_phenolic = f"({'|'.join(PHENOLIC_UNITS)})"

    _re_unit = f"({'|'.join(UNITS)})"
    re_unit = (
        "("
            # fr"{_re_unit}{re_phenolic}?"
            # fr"(((/|per)(\d+)?{_re_unit})|((\d+)?{_re_unit}-1))?"
            fr"{_re_unit}(/(\d+)?{_re_unit})?"
        ")"
    )

    return (
        fr"{re_value}"
        fr"({re_unit}?-{re_value})?"
        fr"{re_unit}{re_weight}?$"
    )


def parse_conc_string(
    row: pd.Series
) -> tuple[float, str, str]:
    """Parse the concentration string without standardizing it.

    Args:
        row (pd.Series): A row of the dataframe.

    Returns:
        tuple[float, str, str]: The concentration value, unit, and weight type.

    """
    # Clean the concentration string.
    if pd.isna(row['_conc']):
        return None, None, None

    conc_str = row['_conc'].replace('·', '')
    for range_ in RANGES:
        conc_str = conc_str.replace(range_, '-')
    conc_str = ''.join(conc_str.split())

    # Check if it matches the regex.
    if not re.fullmatch(_get_regex(), conc_str):
        return None, None, None

    conc_value, conc_unit, conc_weight = None, None, None

    # Ends with weight types.
    for weight_type in WEIGHT_TYPES:
        if conc_str.endswith(weight_type):
            conc_str = conc_str[:-len(weight_type)]
            conc_weight \
                = 'fresh' if weight_type in ['freshweight', 'offw', 'fw'] else 'dry'
            break

    def _separate_conc_value_and_unit(
        conc_str: str
    ) -> tuple[float, str]:
        i = 0
        while i < len(conc_str):
            if conc_str[i].isdigit() or conc_str[i] in ['.', '±']:
                i += 1
            else:
                break
        conc_value = conc_str[:i]
        conc_unit = conc_str[i:]

        if '±' in conc_value:
            conc_value = conc_value.split('±')[0]

        return float(conc_value), conc_unit

    # Range.
    if '-' in conc_str:
        conc_terms = conc_str.split('-')
        if len(conc_terms) == 2:
            conc_first = conc_terms[0]
            conc_second = conc_terms[1]

            # Check if the first term has a unit.
            conc_value_first, conc_unit_first \
                = _separate_conc_value_and_unit(conc_first)
            conc_value_second, conc_unit_second \
                = _separate_conc_value_and_unit(conc_second)

            if conc_unit_first and conc_unit_first != conc_unit_second:
                # Skip these edge cases for now.
                pass
            else:
                conc_value = np.mean([conc_value_first, conc_value_second])
                conc_unit = conc_unit_second
        else:
            raise NotImplementedError()
    else:
        conc_value, conc_unit = _separate_conc_value_and_unit(conc_str)

    return conc_value, conc_unit, conc_weight


def convert_conc_unit(
    row: pd.Series
) -> tuple[float, str]:
    """Standardize the concentration string and return the value and unit.

    Args:
        row (pd.Series): A row of the dataframe.

    Returns:
        tuple[float, str]: The standardized concentration value and unit.

    """
    def _separate_unit_and_factor(conc_unit_str):
        i = 0
        while i < len(conc_unit_str):
            if conc_unit_str[i].isdigit():
                i += 1
            else:
                break
        conc_unit_factor = conc_unit_str[:i]
        conc_unit = conc_unit_str[i:]

        return conc_unit_factor, conc_unit

    if pd.isna(row['_conc_value']) or pd.isna(row['_conc_unit']):
        return None, None

    conc_value, conc_unit = None, None
    _conc_value, _conc_unit = row['_conc_value'], row['_conc_unit']
    _conc_unit_terms = _conc_unit.split('/')

    if len(_conc_unit_terms) == 1:
        _conc_unit = _conc_unit_terms[0]
        _conc_unit_factor, _conc_unit = _separate_unit_and_factor(_conc_unit)
        if _conc_unit_factor:
            # A numerator should not be a unit with factor.
            raise ValueError(f"Invalid conc_unit: {row['_conc_unit']}")

        # Parse units.
        if _conc_unit in ['%']:
            conc_value = _conc_value
            conc_unit = _conc_unit
        elif _conc_unit in UNITS_MASS:
            conc_value = _conc_value * UNITS_MASS[_conc_unit] * 1000
            conc_unit = 'mg'
        elif _conc_unit in UNITS_MOLE:
            conc_value = _conc_value * UNITS_MOLE[_conc_unit]
            conc_unit = 'umol'
        elif _conc_unit in UNITS_ENERGY:
            conc_value = _conc_value * UNITS_ENERGY[_conc_unit] / 4.184
            conc_unit = 'kcal'
        else:
            pass

    elif len(_conc_unit_terms) == 2:
        _conc_unit_factor_num, _conc_unit_num \
            = _separate_unit_and_factor(_conc_unit_terms[0])
        if _conc_unit_factor_num:
            raise ValueError(f"Invalid conc_unit: {row['_conc_unit']}")

        _conc_unit_factor_den, _conc_unit_den \
            = _separate_unit_and_factor(_conc_unit_terms[1])
        if _conc_unit_factor_den:
            _conc_value = _conc_value / float(_conc_unit_factor_den)

        if _conc_unit_num in UNITS_MASS:
            _conc_value = _conc_value * UNITS_MASS[_conc_unit_num] * 1000
            _conc_unit_num = 'mg'
        elif _conc_unit_num in UNITS_VOLUME:
            _conc_value = _conc_value * UNITS_VOLUME[_conc_unit_num] * 1000
            _conc_unit_num = 'ml'
        elif _conc_unit_num in UNITS_MOLE:
            _conc_value = _conc_value * UNITS_MOLE[_conc_unit_num]
            _conc_unit_num = 'umol'
        elif _conc_unit_num in UNITS_ENERGY:
            _conc_value = _conc_value * UNITS_ENERGY[_conc_unit_num] / 4.184
            _conc_unit_num = 'kcal'
        else:
            pass

        if _conc_unit_den in UNITS_MASS:
            _conc_value = _conc_value / UNITS_MASS[_conc_unit_den] * 100
            _conc_unit_den = '100g'
        elif _conc_unit_den in UNITS_VOLUME:
            _conc_value = _conc_value / UNITS_VOLUME[_conc_unit_den] / 1000 * 100
            _conc_unit_den = '100ml'
        elif _conc_unit_den in UNITS_MOLE:
            _conc_value = _conc_value / UNITS_MOLE[_conc_unit_den] * 1e6
            _conc_unit_den = 'mol'
        else:
            pass

        conc_value = _conc_value
        conc_unit = f"{_conc_unit_num}/{_conc_unit_den}"
    else:
        raise ValueError(f"Invalid conc_unit: {conc_unit}")

    return conc_value, conc_unit


def standardize_chemical_conc(
    metadata: pd.DataFrame
) -> pd.DataFrame:
    """Standardize chemical concentration strings.

    Args:
        metadata (pd.DataFrame): The metadata dataframe.

    Returns:
        pd.DataFrame: The standardized metadata dataframe.

    """
    metadata['_conc'] = metadata['_conc'].replace('', np.nan)
    print(f"# metadata entries: {len(metadata)}")
    print(f"# metadata entries with raw conc: {len(metadata.dropna(subset=['_conc']))}")

    # Parse the concentration strings.
    metadata[['_conc_value', '_conc_unit', '_conc_weight_type']] = None
    metadata[['_conc_value', '_conc_unit', '_conc_weight_type']] \
        = metadata.progress_apply(parse_conc_string, axis=1).apply(pd.Series)

    # Standardize the concentration strings.
    metadata[['conc_value', 'conc_unit']] \
        = metadata.progress_apply(convert_conc_unit, axis=1).apply(pd.Series)

    print(f"# metadata entries: {len(metadata)}")
    print(f"# metadata entries with conc: {len(metadata.dropna(subset=['conc_unit']))}")

    return metadata
