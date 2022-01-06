"""
-----------------
 Execute segment
-----------------
"""
from bigquery_interactor import load_data_to_bigquery
from sheet_data_model_generator import generate_model, get_formatted_data_by_model
from xlsx_wrapper import OpenXLSX

import configparser


def main():
    # init config file
    config = configparser.ConfigParser()
    config.read('.ini')

    # read config file
    dataset_id = config['BQ']['DATASET_ID']
    table_id = config['BQ']['TABLE_ID']
    file_url = config['FILE_PLACEMENT']['FILE_URL']

    # try to open cache-file
    xlsx = OpenXLSX.create_by_cached_file(file_url)
    # if unsuccessful
    if xlsx is None:
        # download it from the Google sheets
        xlsx = OpenXLSX.create_by_download_from_google_sheets(file_url)
    # open the cached file
    with xlsx as xlsx_wrapper:
        # read columns names and data (content)
        col_names = xlsx_wrapper.get_sheet_column_names()
        raw_data = xlsx_wrapper.get_rows_data(len(col_names), limit=10)

    # create a model based on analyzing the data
    model = generate_model(col_names, raw_data)
    # format data according to model
    formatted_data = get_formatted_data_by_model(model, col_names, raw_data)

    # load data to bigquery
    response = load_data_to_bigquery(dataset_id, table_id, col_names, model, formatted_data)

    # print response
    print('response of action:', response)


if __name__ == '__main__':
    main()
