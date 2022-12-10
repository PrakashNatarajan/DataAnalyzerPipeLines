#CleaningWorker
import os

def internal_hier_file(src_file_path):
  src_file_one = src_file_path.sub(".csv", "_1.csv")
  src_file_two = src_file_path.sub(".csv", "_2.csv")
  src_file_three = src_file_path.sub(".csv", "_3.csv")
  os.rename(src_file_path, src_file_one)
  rm_non_printables_cmd = _rmv_non_printables().format(src_file_path = src_file_one, dst_file_path = src_file_two)
  rm_double_quotes_one = _rmv_double_quotes().format(src_file_path = src_file_two, dst_file_path = src_file_three)
  rm_double_quotes_two = _rmv_double_quotes().format(src_file_path = src_file_three, dst_file_path = src_file_path)
  os.system(rm_non_printables_cmd) # #To Remove non printable chars
  os.system(rm_double_quotes_one) # #To Remove double quotes
  os.system(rm_double_quotes_two) # #To Remove double quotes
  return src_file_path


def _rmv_non_printables():
  rmv_cmd = "strings < {src_file_path} > {dst_file_path}"
  return rmv_cmd


def _rmv_double_quotes():
  rmv_cmd = """sed s/\\"// < #{src_file_path} > #{dst_file_path}"""
  return rmv_cmd
