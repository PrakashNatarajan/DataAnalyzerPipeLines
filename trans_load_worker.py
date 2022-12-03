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
  client, cursor = database_worker.get_db_client_cursor()
  columns = ", ".join(loader['COLUMNS'])
  placeholders = ', '.join(['%s'] * len(loader['COLUMNS']))
  insert_format = "INSERT INTO %s ( %s ) VALUES ( %s )" % (loader['TABLE'], columns, placeholders)
  #insert_format = "INSERT INTO %s ( %s ) VALUES {values}" % (loader['TABLE'], columns)
  with open(loader['dst_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row_dict in csv_dict_reader:
      row_dict['role_name'] = row_dict['role_user']
      cursor.execute(insert_format, tuple(row_dict.values()))
      #insert_query = insert_format.format(values = str(tuple(row_dict.values())))
      #cursor.execute(insert_query)
      client.commit()


def daf_external_level(loader):
  client, cursor = database_worker.get_db_client_cursor()
  columns = ", ".join(loader['COLUMNS'])
  placeholders = ', '.join(['%s'] * len(loader['COLUMNS']))
  #insert_format = "INSERT INTO %s ( %s ) VALUES ( %s )" % (loader['TABLE'], columns, placeholders)
  insert_format = "INSERT INTO %s ( %s ) VALUES {values}" % (loader['TABLE'], columns)
  with open(loader['dst_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row_dict in csv_dict_reader:
      row_dict['role_level'] = row_dict['role']
      if row_dict['role'] == "RS":
        row_dict['role_user'] = row_dict['rscode']
      else:
        row_dict['role_user'] = "_".join([row_dict['rscode'], row_dict['smn_code']])
      row_dict['role_plg'] = row_dict['plg']
      row_dict.delete('role')
      row_dict.delete('rscode')
      row_dict.delete('smn_code')
      row_dict.delete('smn_name')
      row_dict.delete('plg')
      #cursor.execute(insert_format, tuple(row.values()))
      insert_query = insert_format.format(values = str(tuple(row_dict.values())))
      cursor.execute(insert_query)
      client.commit()
