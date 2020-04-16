import re
import xlrd
import gzip
from tap_gmail_csv.gmail_client.models import File


def generator_wrapper(reader):
    to_return = {}

    header_row = None

    for row in reader:
        if header_row is None:
            header_row = row
            continue

        for index, cell in enumerate(row):
            header_cell = header_row[index]

            formatted_key = header_cell.value

            # remove non-word, non-whitespace characters
            formatted_key = re.sub(r"[^\w\s]", "", formatted_key)

            # replace whitespace with underscores
            formatted_key = re.sub(r"\s+", "_", formatted_key)

            to_return[formatted_key.lower()] = cell.value

        yield to_return


def get_row_iterator(table_spec, file_handle: File):
    if table_spec.get("unzip"):
        raw_stream = gzip.GzipFile(fileobj=file_handle.raw_data)
    else:
        raw_stream = file_handle.raw_data

    workbook = xlrd.open_workbook(on_demand=True, file_contents=raw_stream.read())

    sheet = workbook.sheet_by_name(table_spec["worksheet_name"])

    return generator_wrapper(sheet.get_rows())
