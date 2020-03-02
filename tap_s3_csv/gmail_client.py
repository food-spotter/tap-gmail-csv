import pickle
import base64
import os.path
import email
from io import BytesIO, StringIO
from typing import List, Dict, Generator, Iterable, Any, Union
from dataclasses import dataclass

from googleapiclient.discovery import build, Resource
from apiclient import errors
from google.oauth2.credentials import Credentials


class GoogleAPICredentialsNotFound(Exception):
    pass

class GoogleAPICredentialsAreAnInvalidFormat(Exception):
    pass

# to get round circular dependency - eww
class Message:
    pass

@dataclass
class File:
    """Represents a gmail Attachment File
    """
    file_name: str
    raw_data: BytesIO


@dataclass
class Attachment:
    """Represents a gmail Attachment.
    """
    message_id: str
    attachment_id: str
    attachment_name: str

    def get_file(self, gmail_client) -> File:
        attachment = gmail_client.get_attachment(self.message_id, self.attachment_id)
        file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
        # TODO: preserve file content as bytes. E.g. when a binary file format is attached [zip, xls, xlsx]
        return File(
            self.attachment_name,
            BytesIO(file_data)
        )


@dataclass
class Message:
    """Represents and summarised gmail Message with the juicy bits
    """
    message_id: str
    internal_date: int
    label_ids: List[str] = None
    attachment_list: List[Attachment] = None
    download_urls: List[str] = None
    email_to: str = None
    email_from: str = None
    email_subject: str = None

    def __lt__(self, other):
        return self.internal_date < other.internal_date


class GmailClient():
    DEFAULT_API_VERSION = 'v1'

    def __init__(self, auth_token_path: str, user_id: str='me', api_version: str=DEFAULT_API_VERSION):
        self.user_id = user_id
        self.service = GmailClient._connect(auth_token_path, api_version)

    @staticmethod
    def _get_credentials(token_path: str) -> Credentials:
        creds = None
        if os.path.exists(token_path):
            try:
                with open(token_path, 'rb') as token:
                    creds = pickle.load(token)
            except Exception as e:
                print(e)
                raise GoogleAPICredentialsAreAnInvalidFormat(e)
        else:
            raise GoogleAPICredentialsNotFound(f"File {token_path} was not found.")
        return creds

    @staticmethod
    def _create_client(creds: Credentials, api_version: str, cache_discovery: bool=False) -> Resource:
        return build('gmail', api_version, credentials=creds, cache_discovery=cache_discovery)

    @staticmethod
    def _connect(auth_token_path: str, api_version: str) -> Resource:
        creds = GmailClient._get_credentials(auth_token_path)
        service = GmailClient._create_client(creds, api_version)
        return service

    @staticmethod
    def _convert_to_attachment(message_id: str, part: dict) -> Union[Attachment, None]:
        attachment_obj = None
        if part['filename'] and part.get('body', {}).get('attachmentId'):
            attachment_obj = Attachment(message_id, part['body']['attachmentId'], part['filename'])
        return attachment_obj

    @staticmethod
    def _convert_to_attachment_list(message: dict) -> List[Attachment]:
        attachments = []
        for part in message.get('payload', {}).get('parts', []):
            attachment_obj = GmailClient._convert_to_attachment(message['id'], part)
            if attachment_obj:
                attachments.append(attachment_obj)
        return attachments

    @staticmethod
    def _find_in_header(message: dict, key: str):
        value = None
        key = key.lower()
        for header in message.get('payload', {}). get('headers', []):
            if header.get('name').lower() == key:
                value = header.get('value')
                break
        return value

    @staticmethod
    def _convert_to_message_obj(message: dict) -> Message:
        message_id = message.get('id')
        label_ids = message.get('labelIds')
        internal_date = message.get('internalDate')
        attachment_list = GmailClient._convert_to_attachment_list(message)
        download_urls = [] #  @TODO needs a get_message_raw response then get_message_body()
        email_to = GmailClient._find_in_header(message, 'To'),
        email_from = GmailClient._find_in_header(message, 'From')
        email_subject = GmailClient._find_in_header(message, 'Subject')

        return Message(
            message_id,
            internal_date,
            label_ids,
            attachment_list,
            download_urls,
            email_to,
            email_from,
            email_subject
        )

    
    def get_messages(self, message_list: Iterable, fetch_mode: str='attachment') -> Generator:
        """
        List of Message objects for a given iterable of message_ids, usually in the form of 
        [{id=message_ids, thread_id=anotherIdThatWeDontCare}]
        """
        # @TODO to support url link
        
        for m in message_list:
            if fetch_mode == 'attachment':
                message_dict = self.get_message(m['id'])
                yield self._convert_to_message_obj(message_dict)


    def search(
            self,
            search_query: str='',
            label_ids: List[str]=None,
            include_spam_trash: bool=False,
            results_per_page: int=None,
            max_search_results: int=None) -> Generator:
        """
        Searches gmail for your given criteria.
        Returns an Iterable of {message_ids, thread_id} of the user's mailbox matching the query.

        Args:
          search_query: String used to filter messages returned.
          Eg.- 'from:user@some_domain.com' for Messages from a particular sender.
          label_ids: List of strings to specify which labels to query against
          include_spam_trash: bool
          results_per_page: int - number of results to get back per page request
          max_search_results: int - limit the number of results

        Returns:
          Generator of Messages that match the criteria of the query. Note that the
          returned Messages contains Message IDs, you must use get with the
          appropriate ID to get the details of a Message.
        """
        # https://developers.google.com/gmail/api/v1/reference/users/messages/list

        message_count = 0
        response = None
        page_token = None

        while response is None or 'nextPageToken' in response:

            if response:
                page_token = response['nextPageToken']

            response = self.service.users().messages().list(
                userId=self.user_id,
                q=search_query,
                labelIds=label_ids,
                includeSpamTrash=include_spam_trash,
                maxResults=results_per_page,
                pageToken=page_token
            ).execute()

            if response.get('messages'):
                for message in response['messages']:
                    if max_search_results and message_count >= max_search_results:
                        return
                    message_count += 1
                    yield message

    @staticmethod
    def extract_message_content_from_body(message: dict) -> List[email.message.Message]:
        """
        Extract body content of raw email response.
        It attempts to strip out non useful stuff and bring back the html and txt of the email body
        
        Returns:
            List[email.message.Message] -- [description]
        """
        msg_str = base64.urlsafe_b64decode(message['raw'].encode('UTF-8'))
        mime_msg = email.message_from_bytes(msg_str)

        messageMainType = mime_msg.get_content_maintype()

        if messageMainType == 'multipart':
            for part in mime_msg.get_payload():
                if part.get_content_maintype() == 'multipart':
                    return part.get_payload()
            return ""
        elif messageMainType == 'text':
            return mime_msg.get_payload()

    def get_message(self, message_id: str, return_format: str='full') -> Dict:
        """
        Get GMail Message for the given message id.

        Args:
          message_id: ID of Message to get.
          format: The format to return the message in. 
            Acceptable values are:
            "full": Returns the full email message data with body content parsed in the payload field; the raw field is not used. (default)
            "metadata": Returns only email message ID, labels, and email headers.
            "minimal": Returns only email message ID and labels; does not return the email headers, body, or payload.
            "raw": Returns the full email message data with body content in the raw field as a base64url encoded string; the payload field is not used.

        Returns:
            Dict - as is response from gmail api
        """
        # https://developers.google.com/gmail/api/v1/reference/users/messages/attachments/get
        message = self.service.users().messages().get(
            userId=self.user_id,
            id=message_id,
            format=return_format
        ).execute()

        return message

    def get_attachment(self, message_id: str, attachment_id: str) -> Dict:
        attachment = self.service.users().messages().attachments().get(
            userId=self.user_id,
            messageId=message_id,
            id=attachment_id
        ).execute()
        return attachment

    # this is a bit useless at the moment and will need to be zapped at some point
    def get_attachments_from_message(self, message: Dict, filetype_filter: str=None) -> Generator:
        """For a given gmail message response (non-raw), get all attachments
        
        Arguments:
            message {Dict} -- gmail api response for a message
        
        Keyword Arguments:
            filetype_filter {str} -- filtering filename (default: {None})
        
        Yields:
            Generator -- [description]
        """
        for part in message['payload']['parts']:
            if part['filename'] and (filetype_filter is None or filetype_filter.lower().endswith(filetype_filter.lower())):
                attachment = self.get_attachment(message['id'], part['body']['attachmentId'])
                file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))

                # TODO: preserve file content as bytes. E.g. when a binary file format is attached [zip, xls, xlsx]
                yield File(
                    part['filename'],
                    StringIO(file_data.decode('UTF-8'))
                )

if __name__ == "__main__":
    print('hello')