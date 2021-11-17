"""
------------------------------------
 Sheet data model generator segment
------------------------------------
"""
from typing import Any, List, Dict, Iterable

import numpy as np


def _try_to_int(val) -> bool:
    if val is None:
        return False

    try:
        if val.__class__ is str:
            int(val)
            return True
        else:
            round_val = int(val)
            return round_val == val
    except Exception:
        return False


def _try_to_float(val) -> bool:
    if val is None:
        return False

    try:
        if val.__class__ is str:
            float(val)
            return True
        else:
            round_val = float(val)
            return round_val == val
    except Exception:
        return False


def _get_actual_column_model(val: Any, col_model: List) -> List:
    # set the required parameter
    if val is None:
        col_model[2] = False
        return col_model

    # set the max len parameter
    val_len = len(str(val))
    if col_model[3] < val_len:
        col_model[3] = val_len

    # set type
    if col_model[1] is None:
        if val in (0, 1):
            col_model[1] = bool
        elif _try_to_int(val):
            col_model[1] = int
        elif _try_to_float(val):
            col_model[1] = float
        elif val.__class__ is str:
            col_model[1] = str
        return col_model
    else:
        if col_model[1] == bool:
            if val in (0, 1):
                return col_model
            else:
                col_model[1] = int
        if col_model[1] == int:
            if _try_to_int(val):
                return col_model
            else:
                col_model[1] = float
        if col_model[1] == float:
            if _try_to_float(val):
                return col_model
            else:
                col_model[1] = str

        return col_model


def _set_value_to_column_model(val: Any, col_model: List) -> Any:
    type_ = col_model[1]
    if type_ is None or val is None:
        return None

    if type_ is bool:
        return bool(val)
    elif type_ is int:
        return int(val)
    elif type_ is float:
        return float(val)
    else:
        return str(val)


def get_formatted_data_by_model(model: Dict[str, List], col_names: Iterable[str], data: np.array) -> np.array:
    """
    :param model:
    :param col_names:
    :param data:
    :return: data_formatted_by_model: np.array
    """

    data = data.copy()

    # format data
    for row in data:
        for x, col_name in enumerate(col_names):
            val = row[x]
            col_model = model[col_name]
            try:
                row[x] = _set_value_to_column_model(val, col_model)
            except Exception:
                pass

    return data


def generate_model(col_names: Iterable[str], data: np.array) -> Dict[str, List]:
    """
    :param col_names:
    :param data:
    :return: Dict[col_name: str, model: List[name, type, is_required, max_len]
    """
    model = {}
    for col_name in col_names:
        model[col_name] = [col_name, None, True, 0]

    # set model
    for row in data:
        for x, col_name in enumerate(col_names):
            val = row[x]
            col_model = model[col_name]
            try:
                model[col_name] = _get_actual_column_model(val, col_model)
            except Exception:
                model[col_name] = col_model

    # except max len for bool, int, float
    for key, val in model.items():
        if val[1] is None or val[1] in (int, bool, float):
            del model[key][3]

    return model
