import yaml

def _capture_config_settings():
  configuration_settings = {}
  configs_file_path = "/mnt/d/WorkSpace/PythonProjects/DataAnalyzerPipeLines/dataloaders.yml"
  with open(configs_file_path, "r") as stream:
    try:
      configuration_settings = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
      print(exc)
  return configuration_settings

def fetch_db_configs(db_name):
  captured_configs = _capture_config_settings()
  return captured_configs[db_name]

def fetch_aws_configs(aws_name):
  captured_configs = _capture_config_settings()
  return captured_configs[aws_name]

def fetch_loader_configs(loader_name):
  captured_configs = _capture_config_settings()
  kpi_configs = captured_configs['DATALOADERS'][loader_name]
  kpi_configs['AWS_DIR'] = captured_configs['AWS_SSS']['AWS_DIR']
  kpi_configs['IMPORT_DIR'] = captured_configs['LOCAL_DIR']['IMPORT']
  kpi_configs['REJECT_DIR'] = captured_configs['LOCAL_DIR']['REJECT']
  return kpi_configs
