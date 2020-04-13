import argparse
import json
import sys
import singer

import dateutil
import datetime
from typing import List, Optional

import tap_gmail_csv.gmail

import tap_gmail_csv.s3 as s3
import tap_gmail_csv.conversion as conversion
import tap_gmail_csv.config
import tap_gmail_csv.format_handler

from tap_gmail_csv.logger import LOGGER as logger

from tap_gmail_csv import gmail
from tap_gmail_csv.gmail_client.models import File


def merge_dicts(first, second):
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]

        else:
            to_return[key] = second[key]

    return to_return


def get_sampled_schema_for_table(config, table_spec):
    logger.info("Sampling records to determine table schema.")

    gmail_emails = gmail.get_emails_for_table(config, table_spec)
    # s3_files = s3.get_input_files_for_table(config, table_spec)

    samples = gmail.sample_files(config, table_spec, gmail_emails)

    metadata_schema = {
        "_email_source_bucket": {"type": "string"},
        "_email_source_file": {"type": "string"},
        "_email_source_lineno": {"type": "integer"},
    }

    data_schema = conversion.generate_schema(samples)

    return {"type": "object", "properties": merge_dicts(data_schema, metadata_schema)}


def sync_table(config, state, table_spec, schema: dict):
    table_name = table_spec["name"]
    pattern = table_spec["pattern"]
    source_type = table_spec.get("source_type", "attachment")
    modified_since = dateutil.parser.parse(state.get(table_name, {}).get("modified_since") or config["start_date"])

    logger.info('Syncing table "{}".'.format(table_name))
    logger.info("Getting files since {}.".format(modified_since))

    gmail_emails = gmail.get_emails_for_table(config, table_spec, modified_since)

    # for logging purposes of knowing how many emails to sync, got to turn generator->list
    gmail_emails_list = list(gmail_emails)

    # s3_files = s3.get_input_files_for_table(
    #     config, table_spec, modified_since)

    logger.info("Found {} emails to be synced.".format(len(gmail_emails_list)))

    if not gmail_emails_list or len(gmail_emails_list) == 0:
        return state

    files_list = []
    client = gmail.create_client(config)

    for message in gmail_emails_list:
        if source_type == "attachment":
            message.filter(pattern)
            for att in message.attachment_list:
                files_list.append((message.internal_date, att))

    if len(files_list) == 0:
        return state

    # generate schema on the fly if one has not been provided
    if not schema:
        schema = get_sampled_schema_for_table(config, table_spec)

    # @TODO determine if schema override should be allowed
    override_schema = {"properties": table_spec.get("schema_overrides", {})}
    schema = merge_dicts(schema, override_schema)

    singer.write_schema(table_name, schema, key_properties=table_spec["key_properties"])

    records_streamed = 0
    schema = {}

    for internal_date, attachment in files_list:
        records_streamed += sync_table_file(config, attachment.get_file(client), table_spec, schema)

        state[table_name] = {
            "modified_since": datetime.datetime.fromtimestamp(
                gmail.gmail_timestamp_to_epoch_seconds(internal_date)
            ).isoformat()
        }

        singer.write_state(state)

    logger.info('Wrote {} records for table "{}".'.format(records_streamed, table_name))

    return state


def sync_table_file(config, file_attachment: File, table_spec, schema):
    logger.info('Syncing file "{}".'.format(file_attachment.file_name))

    email_account = config["email_address"]
    table_name = table_spec["name"]

    iterator = tap_gmail_csv.format_handler.get_row_iterator(config, table_spec, file_attachment)

    records_synced = 0

    for row in iterator:
        metadata = {
            "_email_source_address": email_account,
            "_email_source_file": file_attachment.file_name,
            # index zero, +1 for header row
            "_email_source_lineno": records_synced + 2,
        }

        # to_write = [{**conversion.convert_row(row, schema), **metadata}]
        to_write = [{**row, **metadata}]
        singer.write_records(table_name, to_write)
        records_synced += 1

    return records_synced


def ___sync_table_file(config, s3_file, table_spec, schema):
    logger.info('Syncing file "{}".'.format(s3_file))

    email_account = config["email_account"]
    table_name = table_spec["name"]

    iterator = tap_gmail_csv.format_handler.get_row_iterator(config, table_spec, s3_file)

    records_synced = 0

    for row in iterator:
        metadata = {
            "_email_source_address": email_account,
            "_email_source_file": s3_file,
            # index zero, +1 for header row
            "_email_source_lineno": records_synced + 2,
        }

        to_write = [{**conversion.convert_row(row, schema), **metadata}]
        singer.write_records(table_name, to_write)
        records_synced += 1

    return records_synced


def load_state(filename):
    state = {}

    if filename is None:
        return state

    try:
        with open(filename) as handle:
            state = json.load(handle)
    except Exception:
        logger.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError

    return state


def do_sync(args):
    logger.info("Starting sync.")

    config = tap_gmail_csv.config.load(args.config)
    state = load_state(args.state)
    catalog = load_catalog(args.properties) if args.properties else None

    for table in config["tables"]:
        stream = get_selected_stream(catalog, table.get("name"))
        state = sync_table(config, state, table, schema=stream)

    logger.info("Done syncing.")


def do_discover(args):
    logger.info("Starting discover.")

    config = tap_gmail_csv.config.load(args.config)
    streams_list = []

    for table in config["tables"]:
        schema = get_sampled_schema_for_table(config, table)
        # create the stream as a dict
        stream = singer.SchemaMessage(
            stream=table.get("name"), schema=schema, key_properties=table["key_properties"], bookmark_properties=None
        ).asdict()
        # remove the type key-value
        stream.pop("type")
        streams_list.append(stream)

    # write the catalog to stdout
    write_catalog(streams_list)

    logger.info("Done discover.")


def write_catalog(streams_list: List[dict]):
    catalog = {"streams": streams_list}
    sys.stdout.write(json.dumps(catalog) + "\n")
    sys.stdout.flush()


def load_catalog(filename):
    catalog = {}
    try:
        with open(filename) as handle:
            catalog = json.load(handle)
    except Exception:
        logger.fatal("Failed to decode catalog file. Is it valid JSON?")
        raise RuntimeError
    return catalog


def get_selected_stream(catalog: dict, table_name: str) -> Optional[dict]:
    """
    Will search a catalog and locate the schema that matches the given table name.
    It is assumed that the table_spec should correspond to the stream name: 1-to-1 mapping.
    Returns None if not found.

    Arguments:
        catalog {dict} -- a schema catalog, expected as: `{"streams": [stream_dict, ...]}`
        table_name {str} -- name of table_spec entry which corresponds to the `stream` value.

    Returns:
        Optional[dict] -- matching stream schema. None otherwise.
    """
    logger.info(f"Looking for stream: {table_name}")
    for stream in catalog.get("streams", []):
        if stream.get("stream") == table_name:
            logger.info(f"Found stream for: {table_name}")
            return stream
    logger.info(f"No stream found for: {table_name}. Will fall back to inferring schema later in the process.")
    return None


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-s", "--state", help="State file")
    parser.add_argument("-p", "--properties", "--catalog", help="Catalog file with fields selected")
    parser.add_argument("-d", "--discover", help="Discover schema for table spec(s)", action="store_true")

    args = parser.parse_args()

    try:
        if args.discover:
            do_discover(args)
        else:
            do_sync(args)
    except RuntimeError:
        logger.fatal("Run failed.")
        exit(1)


if __name__ == "__main__":
    main()
