# tap-gmail-csv

This is a [Singer](https://singer.io/) tap that reads CSV/Excel attachments/urls from a GMail Mailbox and produces JSON-formatted data following the Singer spec. 

tap-gmail-csv works together with any other Singer Target to move this to any target destination.

Fork of Connor McArthur's (connor@fishtownanalytics.com) [tap-s3-csv](https://circleci.com/gh/fishtown-analytics/tap-s3-csv/)

[Singer](singer.io) tap that produces JSON-formatted data following
the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Work's like [tap-s3-csv](https://circleci.com/gh/fishtown-analytics/tap-s3-csv/) but will read CSV attachment(s) from a Google GMail account.

Changes from [tap-s3-csv](https://circleci.com/gh/fishtown-analytics/tap-s3-csv/):
* Supports discovery with the `-d` `--discover` flags.
* Supports `--catalog` to enforce a fixed schema at runtime instead of relying on inferring one at runtime. The schema selected is based on what table name is given in the `tables` value of the CONFIG file. It expects both to match to correctly pick up the schema from the CATALOG file.
* CONFIG file requires GMail specific values (only new values shown in excerpt below):
    ```javascript
    {
        "email_address": "me@gmail.com", // the gmail account you are connecting to
        "pickle_base64_encoded": "********", // a google generated pickle file encoded in base64 for authentication
        "gmail_search_query": "to: me+finance@gmail.com", // your search query to apply. The date-time filter is auto applied at runtime
        "start_date": "2017-05-01T17:30:00Z", // this is overridden if a STATE file is specified
        "tables": [
            {
                "source_type": "attachment" // supports ['attachment', 'url'] - url should use the `pattern` field to ensure valid download links are fetched(!)
            }
        ]
    }
    ```

### Installation

To run locally, clone this repo, then run:

```bash
python setup.py install
```

### Run

To run for a given configuration:

```
tap-gmail-csv --config configuration.json
```

To run for a given configuration and predefined schema catalog:

```
tap-gmail-csv --config configuration.json --catalog properties.json
```

To discover schema:

```
tap-gmail-csv --config configuration.json --discover
```

### GMail Authentication

To be able to successfully connect and interact with your mailbox, the tap will require
a base64 encoded version of a pre-authenticated Google pickle file. To create this, please follow these steps:

1. [Enable GMail API access](https://developers.google.com/gmail/api/quickstart/python) for your Google email account. 
Once done, you should save the `credentials.json` file as it will be required in the next step.
1. Authenticate your account and generate a base64 encoded version of the Google pickle file by running:
    ```
    tap-gmail-csv --auth-creation credentials.json
    ```
1. The final output of the above command on your shell will look like: 
    ```javascript
    {"pickle_base64_encoded": "gANjZ29..longstring..\nZHEWTnViLg==\n"}
    ```
1. You can copy paste the above base64 encoded string into the `pickle_base64_encoded` field of your CONFIG file.

You now have successfully authenticated tap-gmail-csv to connect to your GMail account.

### How it works

This tap:

 - Searches GMail for attachment files (or downloadable csv/excel links) matching the spec given.
 - [Optionally] Samples 1000 records out of the first five files found to infer datatypes.
 - Iterates through emails, oldest first, finding attachments that match your criteria, outputting data according
   to the generated schema & Singer spec.
 - After completing each email message, it writes the state so that it can be used to enable incremental replication. 

The `url` looks for anchor tag links inside the HTML body of an email. In some cases, the provider of the data you are ingesting may
send the data to you in this manner for security or email file restrictions.

### Example

Given an Email query which returns a single matching attachment file: `data_2020-02-21.csv`

```csv
id,First Name, Last Name
1,Michael,Bluth
2,Lindsay,Bluth Fünke
3,Tobias,Fünke
```

And a config file:

```json
{
    "email_address": "me@gmail.com",
    "pickle_base64_encoded": "gASViwIAAA...etc...E51Yi4=\n",
    "gmail_search_query": "to:me@gmail.com",
    "start_date": "2017-05-01T00:00:00Z",
    "tables": [
        {
            "name": "some-stream-name",
            "pattern": "(.*)\\.csv$",
            "key_properties": [
                "_email_source_file"
            ],
            "format": "csv",
            "source_type": "attachment",
            "delimiter": ",",
            "unzip": false,
            "quoting": "QUOTE_NONE",
        }
    ]
}
```

An output record might look like:

```json
{
  "id": 3,
  "first_name": "Tobias",
  "last_name": "Funke",
  "_email_source_address": "someone@another-email.com",
  "_email_source_file": "data_2020-02-21.csv",
  "_email_source_lineno": 4,
  "_email_extra": null
}
```

### Recommendations

Although the original `tap-s3-csv` which this tap is forked from supports multiple ingestions of different file schemas via the `tables` array list in the CONFIG file,
it is recommended to not put each of your different ingestion jobs are separate elements in this array list. 
Instead, it is better to separate each data ingestion in it's own config file and run the `tap | target` process per config file in your preferred environment (docker container, etc).

### Input File Gotchas

- Input files MUST have a header row.
- Input files MUST have cells fully populated. Missing cells will break the integration. Empty cells
  are handled by the tap.
- If you have the choice, use CSV, not Excel. This tap is able to stream CSV files row-by-row,
  but it cannot stream Excel files. CSV files are more efficient and more reliable.
- This tap can convert datetimes, but it does not infer date-time as an output datatype. If you want
  the tap to convert a field to datetime, you must specify `date-time` as the `_conversion_type` in
  `schema_overrides`. See "Configuration Format" below for more information.

### Configuration Format

See below for an exhaustive list of configuration fields:

```javascript
{

    // the email address your credentials are used for AWS. This is for an audit reason only.
    "email_address": "me@gmail.com",

    // your authentication token for the above account
    "pickle_base64_encoded": "aBase64encodedstringOftheGoogleGeneratedPickleFile=",
    // the gmail label to search against, Optional
    "gmail_label": "INBOX",
    // the gmail query used to filter what you want to bring back. Use standard gmail filter commands - https://support.google.com/mail/answer/7190?hl=en
    "gmail_search_query": "to: me@gmail.com",

    // the start date to use on the first run. the tap outputs an updated state on each
    // run which you can use going forward for incremental replication
    "start_date": "2017-05-01T00:00:00",


    // table definitions. you can specify multiple tables to be pulled from a given
    // bucket.
    "tables": [
        // example csv table definition with schema overrides
        {
            // table name to output
            "name": "bluths_from_csv",

            // you can limit the paths searched in s3 if there are many files in your
            // bucket
            "search_prefix": "csv-exports",

            // pattern to match an attachment filename or download url inside the html message body
            "pattern": "(.*)\\.csv$",

            // primary key for this table. if append only, use:
            //   ["_email_source_file", "_email_source_lineno"]
            "key_properties": ["id"],

            // format, either "csv" or "excel"
            "format": "csv",

            // specify if file to process come from an attachment or a link to be downloaded from the email body ['attachment', 'url']
            // if `url` is used, it's highly recommended you use the `pattern` field to ensure other website links are not picked up
            "source": "attachment",

            // record delimter (optional), defaults to ","
            "delimiter": "|",

            // if true, unzip the file before reading it at a csv
            "unzip": false,

            // if specified, override the default CSV quoting config
            // More info: https://docs.python.org/3/library/csv.html#csv.QUOTE_ALL
            "quoting": "QUOTE_NONE",

            // if the files don't have a header row, you can specify the field names
            "field_names": ["id", "first_name", "last_name"],

            // for any field in the table, you can hardcode the json schema datatype.
            // "_conversion_type" is the type that the tap will try to coerce the field
            // to -- one of "string", "integer", "number", or "date-time". this tap
            // also assumes that all fields are nullable to be more resilient to empty cells.
            "schema_overrides": {
                "id": {
                    "type": ["null", "integer"],
                    "_conversion_type": "integer"
                },

                // if you want the tap to enforce that a field is not nullable, you can do
                // it like so:
                "first_name": {
                    "type": "string",
                    "_conversion_type": "string"
                }
            }
        },

        // example excel definition
        {
            "name": "bluths_from_excel",
            "pattern": "(.*)\\.xlsx$",
            "key_properties": ["id"],
            "format": "excel",

            // the excel definition is identical to csv except that you must specify
            // the worksheet name to pull from in your xls(x) file.
            "worksheet_name": "Names"
        }
    ]
}
```

### Output Format

- Column names have whitespace removed and replaced with underscores.
- They are also downcased.
- A few extra fields are added for help with auditing:
  - `_email_source_address`: The email address that this record came from
  - `_email_source_file`: The path to the file that this record came from
  - `_email_source_lineno`: The line number in the source file that this record was found on
  - `_email_extra`: If you specify field names in the config, and there are more records in a row than field names, the overflow will end up here.
