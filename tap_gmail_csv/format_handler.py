import boto3
import tap_gmail_csv.csv_handler
import tap_gmail_csv.excel_handler


# def get_file_handle(config, s3_path):
#     bucket = config['bucket']
#     s3_client = boto3.resource(
#         's3',
#         aws_access_key_id=config['aws_access_key_id'],
#         aws_secret_access_key=config['aws_secret_access_key'])

#     s3_bucket = s3_client.Bucket(bucket)
#     s3_object = s3_bucket.Object(s3_path)
#     return s3_object.get()['Body']


def get_row_iterator(config, table_spec, file_attachment):

    if table_spec['format'] == 'csv':
        return tap_gmail_csv.csv_handler.get_row_iterator(
            table_spec, file_attachment)

    elif table_spec['format'] == 'excel':
        return tap_gmail_csv.excel_handler.get_row_iterator(
            table_spec, file_attachment)
