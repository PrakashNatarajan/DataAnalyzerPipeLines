from datetime import datetime

def build_source_file_path(configs):
  src_file_path = "{aws_full_dir}/{aws_file_name}_{timestamp}.csv000"
  current_datetime = datetime.now()
  aws_dir_paths = ""
  pre_dir_cnt = configs['AWS_FOLDER'].count("../")
  if pre_dir_cnt > 0:
    aws_dir_paths = configs['AWS_DIR'].split("/")
    aws_dir_paths = "/".join(aws_dir_paths[:-(pre_dir_cnt)])
    aws_folders = configs['AWS_FOLDER'].replace("..","")
    aws_dir_paths += aws_folders
  else:
    aws_dir_paths = configs['AWS_DIR'] + "/" + configs['AWS_FOLDER']
  curr_timestamp = current_datetime.strftime("%Y%m%d")
  src_file_path = src_file_path.format(aws_full_dir = aws_dir_paths, aws_file_name = configs['AWS_FILE'], timestamp = curr_timestamp)
  return src_file_path

def build_destination_file_path(configs):
  dst_file_path = "{dst_full_dir}/{dst_file_name}_{curr_timestamp}.csv"
  dst_full_dir = configs['IMPORT_DIR'] + "/" + configs['LOCAL_FOLDER']
  curr_timestamp = datetime.now().strftime("%Y%m%d_%H%M")
  dst_file_path = dst_file_path.format(dst_full_dir = dst_full_dir, dst_file_name = configs['AWS_FILE'], curr_timestamp = curr_timestamp)
  return dst_file_path

