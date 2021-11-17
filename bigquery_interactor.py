"""
------------------
 BigQuery segment
------------------
"""

from typing import List, Dict
import re

import numpy as np

from google.cloud import bigquery
from google.cloud.bigquery import Dataset, Client

import pandas


def _create_dataset_if_its_not(client: Client, dataset_id: str, location: str = 'US') -> Dataset:
    dataset: Dataset
    try:
        return client.get_dataset(dataset_id)
    except Exception:
        # create dataset
        new_dataset = bigquery.Dataset(dataset_id)
        new_dataset.location = location
        return client.create_dataset(new_dataset)


def _get_unique_table_name(client: Client, dataset_id: str, table_id_name: str, tries_amount: int = 10):
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
            table_id_name = _add_copy_number_to_name(table_id_name)
            table_id = f'{dataset_id}.{table_id_name}'
    else:
        raise ValueError('Too match tries to generate a table name')


def _add_copy_number_to_name(old_name: str) -> str:
    new_name = old_name
    copy_number = re.match(r'^.+?(_(?P<number>[0-9]+))?$', old_name)
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


def _get_schema_kwargs(raw_schema: Dict[str, List], schema_kwargs: List, lambdas: List) -> List[Dict]:
    if len(schema_kwargs) != len(lambdas):
        raise ValueError(f'len of schema_kwargs "{len(schema_kwargs)}" and lambdas "{len(lambdas)}" is not the same')

    result = []

    for key, val in raw_schema.items():
        if len(val) > len(lambdas):
            raise ValueError(f'len of value "{len(val)}" of raw_schema[{key}] is greater than '
                             f'len of lambdas and schema_kwargs "{len(lambdas)}"')

        param_dict = {}
        for i, param in enumerate(val):
            param_dict[schema_kwargs[i]] = lambdas[i](param)
        result.append(param_dict)

    return result


def _create_table(client: Client, table_id: str, schema_kwargs: List[Dict], column_names: List, data: np.array) -> str:
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


def load_data_to_bigquery(dataset_id_name: str,
                          table_id_name: str,
                          column_names: List[str],
                          model: Dict[str, List],
                          data: np.array) -> str:
    # Construct a BigQuery client object.
    with bigquery.Client() as client:

        dataset_id = f'{client.project}.{dataset_id_name}'
        _create_dataset_if_its_not(client, dataset_id)

        table_id_name = _get_unique_table_name(client, dataset_id, table_id_name)
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
        schema = _get_schema_kwargs(model, ['name', 'field_type', 'mode', 'max_length'], [
            lambda name: name,
            lambda type_: appropriate_types[type_],
            lambda mode: 'REQUIRED' if mode else 'NULLABLE',
            lambda max_len: max_len + 1
        ])

        return _create_table(client, table_id, schema, column_names, data)