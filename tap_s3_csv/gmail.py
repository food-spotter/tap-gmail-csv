import os
import re
import base64
import datetime
import dateutil
import tempfile
from typing import Union, Dict, NamedTuple, Generator, Iterator
from operator import attrgetter

from tap_s3_csv.gmail_client import GmailClient, Message, File

from tap_s3_csv.logger import LOGGER as logger
import tap_s3_csv.format_handler


A_HREF_REGEX = r"<a\s+(?:[^>]*?\s+)?href=([\"'])(http.*?)\1"

def _extract_state_from_message(message: Dict) -> Union[int, None]:
    epoch_time_ms = message.get('internalDate')
    epoch_time_sec = int(epoch_time_ms)/1000 if epoch_time_ms else None
    return epoch_time_sec

def _write_temp_file(data) -> tempfile.NamedTemporaryFile:
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.write(data)
    tmp.close()
    return tmp

def _save_pickle_base64_encoded(pickle_base64: str) -> str:
    pickle_bytestring = pickle_base64.encode()
    pickle_data = base64.decodebytes(pickle_bytestring)
    tmp = _write_temp_file(pickle_data)
    return tmp.name

def _unlink_pickle_file(named_temp_file_path: str) -> None:
    os.unlink(named_temp_file_path)

def _create_client(config: dict):
    pickle_file_path = _save_pickle_base64_encoded(config.get('pickle_base64_encoded'))
    client = GmailClient(pickle_file_path)
    _unlink_pickle_file(pickle_file_path)
    return client

def _get_epoch_time(dttm: datetime.datetime) -> int:
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dttm.replace(tzinfo=None) - epoch).total_seconds())

def _get_ordered_messages(search_query: str, results_per_page: int=None, max_search_results: int=None, gmail_client: GmailClient=None):
    if not gmail_client:
        gmail_client = _create_client(config)

    # search gmail to bring back ids 
    message_ids = gmail_client.search(
        search_query=search_query, 
        results_per_page=results_per_page, 
        max_search_results=max_search_results
    )

    messages = gmail_client.get_messages(message_ids)

    # since messages are ordred by thread order descending with no override on this,
    # we need to manually order by message['internalDate']
    messages = list(messages)
    messages.sort()

    return messages

def get_emails_for_table(config: dict, table_spec: dict, modified_since=None, gmail_client: GmailClient=None) -> Generator[Message, None, None]:
    """Returns a list of applicable emails for the given config criteria.
    
    Arguments:
        config {dict} -- singer config
        table_spec {dict} -- found in the config file
    
    Keyword Arguments:
        modified_since {[type]} -- the state value (default: {None})
    
    Returns:
        Generator[Message, None, None] -- a generator that contains Message
    """
    if not gmail_client:
        gmail_client = _create_client(config)

    search_query = config.get('gmail_search_query')
    csv_source = table_spec.get('source', 'attachment')

    # modified_since was the state value for tap-s3-csv, @TODO rename this to something gmail specific
    # convert modified_since to epoch time to use in search_query

    epoch_search_from = _get_epoch_time(
        modified_since if modified_since else dateutil.parser.parse(config.get('start_date'))
        )

    search_query += f" after:{epoch_search_from}"

    pattern = table_spec['pattern']
    matcher = re.compile(pattern)

    logger.info(
        'Checking email using gmail search query: "{}" for files located in "{}" with pattern "{}"'
        .format(search_query, csv_source, pattern))

    messages = _get_ordered_messages(search_query, gmail_client=gmail_client)

    return messages


def sample_file(config, table_spec, file_attachment: File, sample_rate, max_records):
    logger.info('Sampling {} ({} records, every {}th record).'
                .format(file_attachment.file_name, max_records, sample_rate))

    samples = []

    iterator = tap_s3_csv.format_handler.get_row_iterator(
        config, table_spec, file_attachment)

    current_row = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    logger.info('Sampled {} records.'.format(len(samples)))

    return samples


def sample_files(config, table_spec, gmail_emails_list: Iterator[Message],
                 sample_rate=10, max_records=1000, max_files=5):
    to_return = []

    files_so_far = 0

    attached_files = []
    client = _create_client(config)

    for msg in gmail_emails_list:
        for attachment in msg.attachment_list:
            to_return += sample_file(config, table_spec, attachment.get_file(client),
                                    sample_rate, max_records)
            
            files_so_far += 1

            if files_so_far >= max_files:
                break
        if files_so_far >= max_files:
            break

    return to_return


def main():
    client = GmailClient('token.pickle')


    messages = client.search_messages(search_query='to:spotter.food+csvtest@gmail.com after:1582993422', results_per_page=10, max_search_results=10)

    for m in messages:
        message = client.get_message_body(m['id'])
        for m in message:
            print(m)

        state = _extract_state_from_message(m)

        # attachments = client.get_attachments_from_message(message, filetype_filter=".csv")
        #
        #
        # for f in attachments:
        #     print(f)
        #     reader = csv.reader(f['file_content'], delimiter=',')
        #     for row in reader:
        #         print(row)


if __name__ == '__main__':
    main()