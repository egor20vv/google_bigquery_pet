"""
----------------------
 XLSX handler segment
----------------------
"""


import re
from pathlib import Path
from typing import IO, Iterable, List, Dict

import openpyxl
from openpyxl.worksheet.worksheet import Worksheet


class WrapperXLSX:
    # TODO implement

    def get_sheet_column_names(self) -> List:
        _col_names = []
        for _col in self._sheet.iter_cols(max_row=1):
            _col_names.append(_col[0].value)
        return _col_names

    def get_rows_data(self, offset: int = None, limit: int = None) -> List[Dict]:
        data = []

        col_names = self.get_sheet_column_names()

        min_row = offset
        max_row = limit + (offset if offset else 0) if limit else None
        for i, row in enumerate(self._sheet.iter_rows(values_only=True,
                                                      min_row=min_row,
                                                      max_row=max_row,
                                                      max_col=len(col_names))):
            if i == 0:
                pass
                # for name in col_names:
                #     data_by_col[name] = []
            else:
                row_dict = {}
                for j, col in enumerate(row):
                    # data_by_col[col_names[j]].append(col)
                    row_dict[col_names[j]] = col
                data.append(row_dict)

        return data

    def __init__(self, sheet: Worksheet):
        self._sheet = sheet


class OpenXLSX:
    LIMIT_FILE_LEN = 50

    @classmethod
    def get_generated_file_name_from_url(cls, url: str) -> str:
        id_ = re.match(r'^.+/(?P<file_id>[\w-]+)/?$', url).group('file_id')
        return str(id_)[:cls.LIMIT_FILE_LEN] + '.xlsx'

    @classmethod
    def download_from_google_sheets(cls, url: str, path_to_place: str = 'cache\\') -> str:
        """
        :param path_to_place: a path where to a downloaded file will be placed
        :param url: there is a file to download
        :return: file full name to access it
        """
        import requests

        # TODO check url & path_to_place arguments

        id_ = re.match(r'^.+/(?P<file_id>[\w-]+)/?$', url).group('file_id')
        actual_file_url = url if url.endswith('/') else url + '/'

        download_file_request = '{}export?format=xlsx&id={}' \
            .format(actual_file_url, id_)

        with requests.get(download_file_request) as response_data:
            path_to_place = path_to_place if path_to_place.endswith('\\') else path_to_place + '\\'

            with open(path_to_place + str(id_)[:cls.LIMIT_FILE_LEN] + '.xlsx', 'wb') as f:
                if response_data.status_code == 200:
                    print(f'got a response with a status code = {response_data.status_code}')
                    f.write(response_data.content)
                    print('xlsx file successfully stored')
                else:
                    print(f'Bad request: status code = {response_data.status_code}')

        return path_to_place + str(id_)[:cls.LIMIT_FILE_LEN] + '.xlsx'

    def __init__(self, file_name: str):

        match = re.match(r'^(?P<full_path>(.:\\{1,2})?([\w.-]+\\{1,2})*)(?P<name>[\w.-]+[.]xlsx)$', file_name)
        if match:
            self.xlsx_file_name = file_name
            print('full_path:', match.group('full_path'))
            print('file_name:', match.group('name'))

            if not Path(file_name).exists():
                raise ValueError(f'file_name "{file_name}" is not exists')

        else:
            raise ValueError(f'file_name "{file_name}" is not correct')

    def __enter__(self) -> WrapperXLSX:
        self._xlsx = openpyxl.load_workbook(self.xlsx_file_name)

        # get active sheet
        sheet: Worksheet = self._xlsx.active

        return WrapperXLSX(sheet)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, '_xlsx'):
            self._xlsx.close()

    def __del__(self):
        # TODO its might be not correct (study the topic)
        self.__exit__(None, None, None)
