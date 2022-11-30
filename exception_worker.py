from email.utils import formataddr
from smtplib import SMTP, SMTPException
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import configs_worker

def source_not_available(loader):
  loader['subject'] = "Source File Not Available"
  loader = build_message_body(loader)
  configs = configs_worker.fetch_aws_configs('AWS_SMTP')
  loader['message'] = build_complete_message(configs, loader)
  send_exception_mail(configs, loader)

def source_valid_failed(loader):
  loader['subject'] = "Source File Validation Failed"
  loader = build_message_body(loader)
  configs = configs_worker.fetch_aws_configs('AWS_SMTP')
  loader['message'] = build_complete_message(configs, loader)
  send_exception_mail(configs, loader)

def error_while_process(loader):
  loader['subject'] = "Error While Processing Data"
  loader = build_message_body(loader)
  configs = configs_worker.fetch_aws_configs('AWS_SMTP')
  loader['message'] = build_complete_message(configs, loader)
  send_exception_mail(configs, loader)

def build_message_body(loader):
  loader['body_text'] = ("Amazon SES Test - SSL\r\n"
             "This email was sent through the Amazon SES SMTP "
             "Interface using the Python smtplib package.")

  loader['body_html'] = """<html>
                 <head></head>
                 <body>
                   <h3>Source File Name ==> {source_file_name}</h3>
                   <h3>Source Columns ==> {source_columns}</h3>
                   <h3>DST Table Columns ==> {table_columns}</h3>
                   <p>This email was sent with Amazon SES using the
                   <a href='https://www.python.org/'>Python</a>
                   <a href='https://docs.python.org/3/library/smtplib.html'>smtplib</a> library.</p>
                 </body>
              </html>"""
  loader['body_html'].format(source_file_name=loader['src_file_path'], source_columns=loader['source_columns'], table_columns=loader['COLUMNS'])
  return loader

def build_complete_message(configs, loader):
  message = MIMEMultipart('alternative')
  message['Subject'] = loader['subject']
  message['From'] = formataddr((configs['SENDER_NAME'], configs['SENDER_MAIL']))
  message['To'] = ", ".join(configs['RECIPIENTS'])
  # Comment or delete the next line if you are not using a configuration set
  # message.add_header('X-SES-CONFIGURATION-SET',CONFIGURATION_SET)

  # Record the MIME types of both parts - text/plain and text/html.
  part1 = MIMEText(loader['body_text'], 'plain')
  part2 = MIMEText(loader['body_html'], 'html')

  # Attach parts into message container.
  # According to RFC 2046, the last part of a multipart message, in this case
  # the HTML message, is best and preferred.
  message.attach(part1)
  message.attach(part2)
  return message

def send_exception_mail(configs, loader):
  try:
    with SMTP(configs['SERVER']) as server:
      server.starttls()
      server.login(configs['ACCESS_KEY'], configs['SECRET_KEY'])
      server.sendmail(configs['SENDER_MAIL'], configs['RECIPIENTS'], loader['message'].as_string())
      server.close()
      print("Email sent!")
  except SMTPException as error:
    print("Error: ", error)

