"""
-----------------
 Execute segment
-----------------
"""
from bigquery_interactor import load_data_to_bigquery
from sheet_data_model_generator import generate_model, get_formatted_data_by_model
from xlsx_wrapper import OpenXLSX

from bigquery_interactor import load_data_to_bigquery
from sheet_data_model_generator import generate_model, get_formatted_data_by_model
from xlsx_wrapper import OpenXLSX


DATASET_ID = 'my_test_dataset'
TABLE_ID = 'test_table'

FILE_URL = r'https://docs.google.com/spreadsheets/d/1E3w-YesqOOyxti2tN-DL-0VWbyas0aHzLzjKgh-JN-A/'


def main():

    xlsx = OpenXLSX.create_by_cached_file(FILE_URL)
    if xlsx is None:
        xlsx = OpenXLSX.create_by_download_from_google_sheets(FILE_URL)
    with xlsx as xlsx_wrapper:
        col_names = xlsx_wrapper.get_sheet_column_names()
        raw_data = xlsx_wrapper.get_rows_data(len(col_names), limit=10)

    model = generate_model(col_names, raw_data)
    formatted_data = get_formatted_data_by_model(model, col_names, raw_data)

    response = load_data_to_bigquery(DATASET_ID, TABLE_ID, col_names, model, formatted_data)
    print('response of action:', response)


if __name__ == '__main__':
    main()
