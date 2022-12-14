import configs_worker
import mysql.connector as mysql_client


def fetch_record_query(loader):
  cursor = _connection_client_cursor()
  result = cursor.fetchall()

def create_record_query(table, columns, values):
  cursor = _connection_client_cursor()


def update_record_query(table, columns, values):
  cursor = _connection_client_cursor()


def delete_record_query(table, columns, values):
  cursor = _connection_client_cursor()


def add_missed_user_data(table):
  cursor = _connection_client_cursor()


def assign_user_group(table):
  cursor = _connection_client_cursor()


def get_db_client_cursor():
  client, cursor = _connection_client_cursor()
  return client, cursor

def _connection_client_cursor():
  configs = configs_worker.fetch_db_configs(db_name="ICUBEDB")
  connection = mysql_client.connect(host=configs['HOST'], user=configs['USERNAME'], password=configs['PASSWORD'], database=configs['DATABASE'], auth_plugin='mysql_native_password', port=configs['PORT'])
  cursor = connection.cursor()
  return connection, cursor