import csv


def user_auth_master(loader):
  with open(loader['src_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def user_proxy_master(loader):
  with open(loader['src_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def internal_role_hierarchy(loader):
  with open(loader['src_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def external_role_hierarchy(loader):
  with open(loader['src_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def daf_internal_level(loader):
  with open(loader['src_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)


def daf_external_level(loader):
  with open(loader['src_file_path'], 'r') as read_obj:
    #csv_dict_reader = csv.DictReader(read_obj) # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = csv.DictReader(read_obj, delimiter='|')  # With delimiter
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
      print(row)

