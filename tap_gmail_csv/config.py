import json

from tap_gmail_csv.logger import LOGGER as logger
from voluptuous import Schema, Required, Any, Optional

CONFIG_CONTRACT = Schema({
    Required('email_address'): str,
    Required('pickle_base64_encoded'): str,
    Optional('gmail_label'): str,
    Required('gmail_search_query'): str,
    Required('start_date'): str,
    Required('email_address'): str,
    Required('tables'): [{
        Required('name'): str,
        Required('pattern'): str,
        Required('key_properties'): [str],
        Required('format'): Any('csv', 'excel'),
        Required('source'): Any('attachment', 'url'),
        Optional('unzip'): bool,
        Optional('delimiter'): str,
        Optional('quoting'): Any('QUOTE_MINIMAL', 'QUOTE_ALL', 'QUOTE_NONNUMERIC', 'QUOTE_NONE'),
        Optional('search_prefix'): str,
        Optional('field_names'): [str],
        Optional('worksheet_name'): str,
        Optional('schema_overrides'): {
            str: {
                Required('type'): Any(str, [str]),
                Required('_conversion_type'): Any('string',
                                                  'integer',
                                                  'number',
                                                  'date-time')
            }
        }
    }]
})


def load(filename):
    config = {}

    try:
        with open(filename) as handle:
            config = json.load(handle)
    except:
        logger.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError

    CONFIG_CONTRACT(config)

    return config
