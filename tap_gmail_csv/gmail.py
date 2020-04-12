import os
import re
import base64
import datetime
from dateutil import parser
import tempfile
from typing import List, Union, Dict, Generator, Iterator, Any

from tap_gmail_csv.gmail_client.client import GmailClient
from tap_gmail_csv.gmail_client.models import Message, File

from tap_gmail_csv.logger import LOGGER as logger
import tap_gmail_csv.format_handler


A_HREF_REGEX = r"<a\s+(?:[^>]*?\s+)?href=([\"'])(http.*?)\1"


def gmail_timestamp_to_epoch_seconds(epoch_time_ms: int) -> int:
    """
    Convert GMail `internalDate` into epoch time in seconds.
    
    Arguments:
        epoch_time_ms {int} -- the GMail `internalDate` epoch time which is in milliseconds.
    
    Returns:
        int -- epoch time in seconds
    """
    epoch_time_sec = int(epoch_time_ms) / 1000
    return epoch_time_sec


def _extract_state_from_message(message: Dict) -> Union[int, None]:
    """
    Extracts the gmail `internalDate` for the given google gmail low level `message`
    resource in epoch time (seconds). This value can be used as the state id.

    Arguments:
        message {Dict} -- low level google gmail `message` resource.

    Returns:
        Union[int, None] -- epoch time in seconds
    """
    epoch_time_ms = message.get("internalDate")
    epoch_time_sec = gmail_timestamp_to_epoch_seconds(epoch_time_ms) if epoch_time_ms else None
    return epoch_time_sec


def _write_temp_file(data: Any) -> tempfile._TemporaryFileWrapper:  # type: ignore
    """
    Writes a named temporary file with the given data.

    Returns:
        [TemporaryFileWrapper] -- a file handle to the created temp file
    """
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.write(data)
    tmp.close()
    return tmp


def _save_pickle_base64_encoded(pickle_base64: str) -> str:
    """
    Save the base64 encoded pickle file as a temp file.

    Arguments:
        pickle_base64 {str} -- pickle file that is base64 encoded

    Returns:
        str -- name of the newly created temp pickle file
    """
    pickle_bytestring = pickle_base64.encode()
    pickle_data = base64.decodebytes(pickle_bytestring)
    tmp = _write_temp_file(pickle_data)
    return tmp.name


def _get_epoch_time(dttm: datetime.datetime) -> int:
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dttm.replace(tzinfo=None) - epoch).total_seconds())


def _get_ordered_messages(
    gmail_client: GmailClient, search_query: str, results_per_page: int = None, max_search_results: int = None
) -> List[Message]:
    """
    Get matching Messages in date order DESCending.

    Arguments:
        gmail_client {GmailClient} -- GmailClient to carry out operation.
        search_query {str} -- search query to apply.

    Keyword Arguments:
        results_per_page {int} -- number of results to get back per page request. (default: {None})
        max_search_results {int} -- limit the number of results. (default: {None})

    Returns:
        List[Message] -- a complete List of Message objects that match the given search criteria.
    """

    # search gmail to bring back ids
    messages = gmail_client.search(
        search_query=search_query, results_per_page=results_per_page, max_search_results=max_search_results
    )

    # Since messages are ordered by thread order descending, with no override on this.
    # So we need to manually order by message['internalDate']
    ordered_messages = list(messages)
    ordered_messages.sort()

    return ordered_messages


def create_client(config: dict) -> GmailClient:
    pickle_base64_encoded = config.get("pickle_base64_encoded", "")
    pickle_file_path = _save_pickle_base64_encoded(pickle_base64_encoded)
    try:
        client = GmailClient(pickle_file_path)
    finally:
        os.unlink(pickle_file_path)
    return client


def get_emails_for_table(
    config: dict, table_spec: dict, modified_since: datetime.datetime = None, gmail_client: GmailClient = None
) -> List[Message]:
    """Returns a list of applicable emails for the given config criteria (aka table).

    Arguments:
        config {dict} -- singer config
        table_spec {dict} -- found in the config file

    Keyword Arguments:
        modified_since {datetime} -- the state value (default: {None})

    Returns:
        List[Message] -- List of Message objects
    """
    if not gmail_client:
        gmail_client = create_client(config)

    search_query = config.get("gmail_search_query", "")
    csv_source = table_spec.get("source", "attachment")

    # modified_since was the state value for tap-s3-csv, @TODO rename this to something gmail specific

    search_from_date = modified_since if modified_since else parser.parse(config.get("start_date", "1 Jan 1970"))
    logger.info(f"Create filter to look for emails after {search_from_date}")

    # convert modified_since to epoch time to use in search_query
    epoch_search_from = _get_epoch_time(search_from_date)
    # append the `after` filter tp the gmail search query
    search_query += f" after:{epoch_search_from}"

    # @TODO apply pattern match on file attachments
    # pattern = table_spec["pattern"]
    # matcher = re.compile(pattern)

    logger.info(f'Checking email using gmail search query: "{search_query}" for files located in "{csv_source}"')

    messages = _get_ordered_messages(gmail_client, search_query)

    return messages


def sample_file(
    config: dict, table_spec: dict, file_attachment: File, sample_rate: int, max_records: int
) -> List[Dict]:
    """
    Sample rows of a file.
    This function remains the same as the original s.3 version
    
    Arguments:
        config {dict} -- singer config
        table_spec {dict} -- found in the config file
        file_attachment {File} -- File object to be processed
        sample_rate {int} -- Row intervals
        max_records {int} -- Maximum number of records to sample
    
    Returns:
        List[Dict] -- sampled rows
    """
    logger.info(f"Sampling {file_attachment.file_name} ({max_records} records, every {sample_rate}th record).")

    samples = []

    iterator = tap_gmail_csv.format_handler.get_row_iterator(config, table_spec, file_attachment)
    current_row = 0

    for row in iterator:
        if (current_row % sample_rate) == 0:
            samples.append(row)

        current_row += 1

        if len(samples) >= max_records:
            break

    logger.info("Sampled {} records.".format(len(samples)))

    return samples


def sample_files(
    config: dict, table_spec: dict, gmail_emails_list: Iterator[Message], sample_rate=10, max_records=1000, max_files=5
) -> List[Dict]:
    """[summary]

    Arguments:
        config {dict} -- singer config
        table_spec {dict} -- found in the config file
        gmail_emails_list {Iterator[Message]} -- All Messages to check against 

    Keyword Arguments:
        sample_rate {int} -- Row intervals
        max_records {int} -- Maximum number of records to sample per File
        max_files {int} -- Maximum number of Files to scan (default: {5})

    Returns:
        List[Dict] -- sampled rows
    """
    pattern = table_spec["pattern"]
    matcher = re.compile(pattern)

    to_return: List[Dict] = []

    files_so_far = 0

    client = create_client(config)

    for msg in gmail_emails_list:
        for attachment in msg.attachment_list:
            # do filename pattern check
            if matcher.search(attachment.attachment_name):
                to_return += sample_file(config, table_spec, attachment.get_file(client), sample_rate, max_records)

                files_so_far += 1
                if files_so_far >= max_files:
                    break
        if files_so_far >= max_files:
            break

    return to_return


def main():
    client = GmailClient("token.pickle")
    messages = client.search_messages(
        search_query="to:spotter.food+csvtest@gmail.com after:1582993422", results_per_page=10, max_search_results=10
    )

    for m in messages:
        message = client.get_message_body(m["id"])
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


if __name__ == "__main__":
    main()
