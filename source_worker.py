import boto3
import botocore
import configs_worker

def check_file_status(loader):
  src_file_status = False
  try:
    object = _build_sss_object(loader['src_file_path'])
    object.load()
    src_file_status = True
  except botocore.exceptions.ClientError as error:
    print(error.response)
  return src_file_status


def download_source_file(loader):
  object = _build_sss_object(loader['src_file_path'])
  object.download_file(loader['dst_file_path'])


def _build_sss_bucket():
  configs = configs_worker.fetch_aws_configs('AWS_SSS')
  resource = boto3.resource('s3', aws_access_key_id = configs['ACCESS_KEY'], aws_secret_access_key = configs['SECRET_KEY'], region_name = configs['REGION'])
  bucket = resource.Bucket(configs['BUCKET'])
  return bucket 


def _build_sss_object(src_file_path):
  bucket = _build_sss_bucket()
  object = bucket.Object(src_file_path)
  return object 
