import csv
import database_worker

def user_auth_master(loader):
  with open(loader['dst_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def user_proxy_master(loader):
  with open(loader['dst_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def internal_role_hierarchy(loader):
  with open(loader['dst_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def external_role_hierarchy(loader):
  with open(loader['dst_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def daf_internal_level(loader):
  cursor = database_worker.get_db_client_cursor()
  columns = ", ".join(loader['COLUMNS'])
  placeholders = ', '.join(['%s'] * len(loader['COLUMNS']))
  #insert_format = "INSERT INTO %s ( %s ) VALUES ( %s )" % (loader['TABLE'], columns, placeholders)
  insert_format = "INSERT INTO %s ( %s ) VALUES {values}" % (loader['TABLE'], columns)
  with open(loader['dst_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      #cursor.execute(insert_format, tuple(row.values()))
      insert_query = insert_format.format(values = str(tuple(row_dict.values())))
      cursor.execute(insert_query)


def daf_external_level(loader):
  with open(loader['dst_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)

