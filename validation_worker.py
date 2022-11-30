from csv import DictReader


def right_column_names(loader):
  valid_columns = False
  column_names = _fetch_header_columns(loader['dst_file_path'])
  equal_columns = column_names == loader['COLUMNS']
  mapped_columns = zip(tuple(column_names), tuple(loader['COLUMNS']))
  mapped_columns = list(mapped_columns)
  matched_columns = all([zip_colm[0] == zip_colm[1] for zip_colm in mapped_columns])
  if equal_columns == matched_columns:
    valid_columns = True
  return valid_columns


def fetch_column_names(loader):
  column_names = _fetch_header_columns(loader['dst_file_path'])
  return column_names


def _fetch_header_columns(file_path):
  # open file in read mode
  column_names = []
  with open(file_path, mode = 'r', encoding = 'ISO8859-1') as read_obj:  # with encoding
  #with open(file_path, mode = 'r', encoding = 'utf-8') as read_obj:  # with encoding
    # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = DictReader(read_obj, delimiter='|')  # With delimiter
    # get column names from a csv file
    column_names = csv_dict_reader.fieldnames
  return column_names

