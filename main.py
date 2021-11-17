from typing import Any, Tuple, List, Dict, Iterable
import re

from xlsx_wrapper import OpenXLSX

import numpy as np

from google.cloud import bigquery
from google.cloud.bigquery import Dataset, Client, Table

import pandas


"""
----------------------------------------
 Generate model over sheet data segment
----------------------------------------
"""


def try_to_int(val) -> bool:
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


def try_to_float(val) -> bool:
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


def get_actual_column_model(val: Any, col_model: List) -> List:
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
        elif try_to_int(val):
            col_model[1] = int
        elif try_to_float(val):
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
            if try_to_int(val):
                return col_model
            else:
                col_model[1] = float
        if col_model[1] == float:
            if try_to_float(val):
                return col_model
            else:
                col_model[1] = str

        return col_model


def set_data_to_column_model(val: Any, col_model: List) -> Any:
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


def get_model_n_format_data(col_names: Iterable[str], data: List[Dict]) -> Tuple[Dict, List[Dict]]:
    """
    :param col_names:
    :param data:
    :return: Tuple [model: Tuple[name, type, required, max_len], data_works_by_model: List[Dict]]
    """
    # set default model
    model = {}
    for col_name in col_names:
        model[col_name] = [col_name, None, True, 0]

    # set model
    for row in data:
        for col_name in col_names:
            val = row[col_name]
            col_model = model[col_name]
            try:
                model[col_name] = get_actual_column_model(val, col_model)
            except Exception:
                model[col_name] = col_model

    # format data
    for row in data:
        for col_name in col_names:
            val = row[col_name]
            col_model = model[col_name]
            try:
                row[col_name] = set_data_to_column_model(val, col_model)
            except Exception:
                pass

    # except max len for bool, int, float
    for key, val in model.items():
        if val[1] is None or val[1] in (int, bool, float):
            del model[key][3]

    return model, data


"""
------------------
 BigQuery segment
------------------
"""


def create_dataset_if_its_not(client: Client, dataset_id: str, location: str = 'US') -> Dataset:
    dataset: Dataset
    try:
        return client.get_dataset(dataset_id)
    except Exception:
        # create dataset
        new_dataset = bigquery.Dataset(dataset_id)
        new_dataset.location = location
        return client.create_dataset(new_dataset)


def get_unique_table_name(client: Client, dataset_id: str, table_id_name: str, tries_amount: int = 10):
    table_id = f'{dataset_id}.{table_id_name}'

    for i in range(tries_amount):
        try:
            table = client.get_table(table_id)
            # printable data is true
            print(f'table "{table_id_name}" is already created')
        except Exception:
            return table_id_name
        else:
            # table already created
            # changes table_id (rename):
            table_id_name = add_copy_number_to_name(table_id_name)
            table_id = f'{dataset_id}.{table_id_name}'
    else:
        raise ValueError('Too match tries to generate a table name')


def add_copy_number_to_name(old_name: str) -> str:
    new_name = old_name
    copy_number = re.match(r'^.+(_(?P<number>[\d]+))?$', old_name)
    if copy_number:
        old_copy_number = copy_number.group('number')
        if old_copy_number:
            new_copy_number = int(old_copy_number) + 1
            # new_name = old_name.removesuffix(old_copy_number) + str(new_copy_number)
            new_name = old_name.replace('_' + old_copy_number, '_' + str(new_copy_number))
        else:
            new_name += '_1'
    else:
        raise ValueError('Wrong match')

    return new_name


def get_schema_kwargs(raw_schema: Dict[str, List], schema_kwargs: List, lambdas: List) -> List[Dict]:
    # if len(raw_schema) != len(schema_kwargs):
    #     raise ValueError(f'len of all argument lists must be the same:\n'
    #                      f'raw_schema len - {len(raw_schema)},\n'
    #                      f'schema_kwargs len - {len(schema_kwargs)},\n'
    #                      f'lambdas len - {len(lambdas)}')

    result = []

    for key, val in raw_schema.items():
        param_dict = {}
        for i, param in enumerate(val):
            param_dict[schema_kwargs[i]] = lambdas[i](param)
        result.append(param_dict)

    return result


def create_table(client: Client, table_id: str, schema_kwargs: List[Dict], column_names: List, data: np.array) -> str:
    try:
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(**kwargs) for kwargs in schema_kwargs]
        )
        data_frame = pandas.DataFrame(data, columns=column_names)
        load_job = client.load_table_from_dataframe(data_frame, table_id, job_config=job_config)

        load_job.result()

        return 'data successfully placed to ' + table_id
    except Exception as e:
        return str(e)


def load_to_bigquery(dataset_id_name: str,
                     table_id_name: str,
                     column_names: List[str],
                     model: Dict[str, List],
                     data: np.array) -> str:
    # Construct a BigQuery client object.
    client = bigquery.Client()

    dataset_id = f'{client.project}.{dataset_id_name}'

    dataset: Dataset
    table: Table

    dataset = create_dataset_if_its_not(client, dataset_id)

    table_id_name = get_unique_table_name(client, dataset_id, table_id_name)
    table_id = f'{dataset_id}.{table_id_name}'

    # create table:
    # create schema
    appropriate_types = {
        bool: 'BOOL',
        int: 'INTEGER',
        float: 'FLOAT',
        str: 'STRING',
        None: 'STRING'
    }
    schema = get_schema_kwargs(model,
                               ['name', 'field_type', 'mode', 'max_length'],
                               [
                                   lambda name: name,
                                   lambda type_: appropriate_types[type_],
                                   lambda mode: 'REQUIRED' if mode else 'NULLABLE',
                                   lambda max_len: max_len + 1
                               ])

    return create_table(client, table_id, schema, column_names, data)


"""
-----------------
 Execute segment
-----------------
"""


DATASET_ID = 'my_test_dataset'

TABLE_ID = 'test_table'


def list_of_dicts_to_np_array(data: List[Dict]) -> np.array:
    result = []
    for row in data:
        result_row = []
        for key, val in row.items():
            result_row.append(val)
        result.append(result_row)
    return np.array(result)


def main():
    file_url = r'https://docs.google.com/spreadsheets/d/1E3w-YesqOOyxti2tN-DL-0VWbyas0aHzLzjKgh-JN-A/'

    xlsx = OpenXLSX.create_by_cached_file(file_url)
    if not xlsx:
        xlsx = OpenXLSX.create_by_download_from_google_sheets(file_url)
    with xlsx as xlsx_wrapper:
        col_names = xlsx_wrapper.get_sheet_column_names()
        raw_data = xlsx_wrapper.get_rows_data()

    model, formatted_data = get_model_n_format_data(col_names, raw_data)

    lilt_of_lists_data = list_of_dicts_to_np_array(formatted_data)
    response = load_to_bigquery(DATASET_ID, TABLE_ID, col_names, model, lilt_of_lists_data)
    print('response of action:', response)


if __name__ == '__main__':
    main()
