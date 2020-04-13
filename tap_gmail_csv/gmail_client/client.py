import pickle
import base64
import os.path
import email
from io import BytesIO
from typing import List, Dict, Generator, Iterable, Optional, Union

from googleapiclient.discovery import build, Resource
from google.oauth2.credentials import Credentials

from tap_gmail_csv.gmail_client.models import Message, File, Attachment


class GoogleAPICredentialsNotFound(Exception):
    pass


class GoogleAPICredentialsAreAnInvalidFormat(Exception):
    pass


class GmailClient:
    """
    Simple GMail client wrapper that supports operations to search a GMail Inbox,
    retrieve message attachments and body content.

    Abstract out GMail library representations into more simple dataclasses
    for representations of a `Message`, `File` and `Attachment`

    Raises:
        GoogleAPICredentialsAreAnInvalidFormat: [description]
        GoogleAPICredentialsNotFound: [description]
    """

    DEFAULT_API_VERSION = "v1"

    def __init__(self, auth_token_path: str, user_id: str = "me", api_version: str = DEFAULT_API_VERSION):
        self.user_id = user_id
        self.service = GmailClient._connect(auth_token_path, api_version)

    @staticmethod
    def _get_credentials(token_path: str) -> Credentials:
        """
        Load credentials from a pickle file

        Arguments:
            token_path {str} -- path to the pickle file

        Raises:
            GoogleAPICredentialsAreAnInvalidFormat: [description]
            GoogleAPICredentialsNotFound: [description]

        Returns:
            Credentials
        """
        creds = None
        if os.path.exists(token_path):
            try:
                with open(token_path, "rb") as token:
                    creds = pickle.load(token)
            except Exception as e:
                print(e)
                raise GoogleAPICredentialsAreAnInvalidFormat(e)
        else:
            raise GoogleAPICredentialsNotFound(f"File {token_path} was not found.")
        return creds

    @staticmethod
    def _create_client(creds: Credentials, api_version: str, cache_discovery: bool = False) -> Resource:
        """
        Creates the resource to interact with the GMail API

        Arguments:
            creds {Credentials} -- Credentials loaded from a pickle file.
            api_version {str} -- which version of the GMail API to use.

        Keyword Arguments:
            cache_discovery {bool} -- whether or not to cache the discovery doc (default: {False})

        Returns:
            Resource -- the resource to interact with the GMail API.
        """
        return build("gmail", api_version, credentials=creds, cache_discovery=cache_discovery)

    @staticmethod
    def _connect(auth_token_path: str, api_version: str) -> Resource:
        """
        Authenticate and return a GMail API resource to interact with.
        The pickle file can be generated as per instructions:
        https://developers.google.com/gmail/api/quickstart/python

        Arguments:
            auth_token_path {str} -- path to a pickle file.
            api_version {str} -- which version of the GMail API to use.

        Returns:
            Resource -- the resource to interact with the GMail API.
        """
        creds = GmailClient._get_credentials(auth_token_path)
        service = GmailClient._create_client(creds, api_version)
        return service

    @staticmethod
    def _convert_to_attachment(message_id: str, part: dict) -> Union[Attachment, None]:
        """
        Convert the given `parts` portion of a raw GMail response to an `Attachment` object if
        it's a valid attachment.

        Returns `None` otherwise.

        Arguments:
            message_id {str} -- id of the GMail message.
            part {dict} -- parts section of a raw gmail response.

        Returns:
            Union[Attachment, None]
        """
        attachment_obj = None
        if part["filename"] and part.get("body", {}).get("attachmentId"):
            attachment_obj = Attachment(message_id, part["body"]["attachmentId"], part["filename"])
        return attachment_obj

    @staticmethod
    def _convert_to_attachment_list(message: dict) -> List[Attachment]:
        """
        Gets a List of `Attachment`s for a given raw GMail response.

        Arguments:
            message {dict} -- the raw GMail response from the `messages` resource.

        Returns:
            List[Attachment]
        """
        attachments = []
        for part in message.get("payload", {}).get("parts", []):
            attachment_obj = GmailClient._convert_to_attachment(message["id"], part)
            if attachment_obj:
                attachments.append(attachment_obj)
        return attachments

    @staticmethod
    def _find_in_header(message: dict, key: str) -> Optional[str]:
        """
        Extract the key value from the header data of a raw GMail api response of a `messages` resource.
        Lookup is case insensitive.

        Arguments:
            message {dict} -- the raw GMail response from the `messages` resource.
            key {str} -- the key to lookup

        Returns:
            Optional[str] -- the value for the given key. `None` if not found
        """
        value = None
        key = key.lower()
        for header in message.get("payload", {}).get("headers", []):
            if header.get("name").lower() == key:
                value = header.get("value")
                break
        return value

    @staticmethod
    def _convert_to_message_obj(message: dict) -> Message:
        message_id = message.get("id", "")
        label_ids = message.get("labelIds")
        internal_date = message.get("internalDate", 0)
        attachment_list = GmailClient._convert_to_attachment_list(message)
        download_urls: List[str] = []  # @TODO needs a get_message_raw response then get_message_body()
        email_to = GmailClient._find_in_header(message, "To")
        email_from = GmailClient._find_in_header(message, "From")
        email_subject = GmailClient._find_in_header(message, "Subject")

        return Message(
            message_id, internal_date, label_ids, attachment_list, download_urls, email_to, email_from, email_subject
        )

    def _get_messages(self, message_list: Iterable) -> Generator[Message, None, None]:
        """
        List of Message objects for a given iterable of message_ids, usually in the form of
        [{id=message_ids, thread_id=anotherIdThatWeDontCare}]

        Arguments:
            message_list {Iterable} -- An iterable that contains the low level google gmail message resource
                                       represented like:
                                       `{"id": "2715e11441a6d424", "threadId": "2715e11441a6d424"}`

        Yields:
            Generator[Message, None, None] -- [description]
        """
        # @TODO to support url link

        for m in message_list:
            message_dict = self._raw_gmail_message(m["id"])
            yield self._convert_to_message_obj(message_dict)

    def search(
        self,
        search_query: str = "",
        label_ids: List[str] = None,
        include_spam_trash: bool = False,
        results_per_page: int = None,
        max_search_results: int = None,
    ) -> Generator[Message, None, None]:
        """
        Search GMail inbox.

        Keyword Arguments:
            search_query {str} -- String used to filter messages returned. (default: {""})
            label_ids {List[str]} -- List of strings to specify which labels to query against. (default: {None})
            include_spam_trash {bool} -- To include messages in spam and trash. (default: {False})
            results_per_page {int} -- number of results to get back per page request. (default: {None})
            max_search_results {int} -- limit the number of results. (default: {None})

        Returns:
            Generator[Message, None, None] -- Generator of Message objects matching your search criteria.
        """
        message_ids = self._raw_search_gmail(
            search_query, label_ids, include_spam_trash, results_per_page, max_search_results
        )

        return self._get_messages(message_ids)

    def _raw_search_gmail(  # pragma: no cover
        self,
        search_query: str = "",
        label_ids: List[str] = None,
        include_spam_trash: bool = False,
        results_per_page: int = None,
        max_search_results: int = None,
    ) -> Generator[Dict[str, str], None, None]:
        """
        Searches GMail for given criteria.

        Yields an Iterable of a raw GMail `message` resource: (`{message_ids, thread_id}`)
        for the matching search query.

        Keyword Arguments:
            search_query {str} -- String used to filter messages returned. (default: {""})
            label_ids {List[str]} -- List of strings to specify which labels to query against. (default: {None})
            include_spam_trash {bool} -- To include messages in spam and trash. (default: {False})
            results_per_page {int} -- number of results to get back per page request. (default: {None})
            max_search_results {int} -- limit the number of results. (default: {None})

        Yields:
            Generator -- a low level google gmail message resource contains only an id and a threadId represented like:
                         `{"id": "2715e11441a6d424", "threadId": "2715e11441a6d424"}`
        """

        message_count = 0
        response = None
        page_token = None

        while response is None or "nextPageToken" in response:
            if response:
                page_token = response["nextPageToken"]

            response = self._raw_gmail_list_messages(
                search_query=search_query,
                label_ids=label_ids,
                include_spam_trash=include_spam_trash,
                results_per_page=results_per_page,
                max_search_results=max_search_results,
                page_token=page_token,
            )

            if response.get("messages"):
                for message in response["messages"]:
                    if max_search_results and message_count >= max_search_results:
                        return
                    message_count += 1
                    yield message

    def _raw_gmail_list_messages(  # pragma: no cover
        self,
        search_query: str,
        label_ids: Optional[List[str]],
        include_spam_trash: Optional[bool],
        results_per_page: Optional[int],
        max_search_results: Optional[int],
        page_token: Optional[str],
    ) -> Dict:
        """
        Lists the messages in the GMail mailbox for given criteria. Example response format:
        https://developers.google.com/gmail/api/v1/reference/users/messages/list#response_1

        Arguments:
            search_query {str} -- String used to filter messages returned.
            label_ids {Optional[List[str]]} -- List of strings to specify which labels to query against.
            include_spam_trash {Optional[bool]} -- To include messages in spam and trash.
            results_per_page {Optional[int]} -- number of results to get back per page request.
            max_search_results {Optional[int]} -- limit the number of results.
            page_token {Optional[str]} -- Page token to retrieve a specific page of results in the list.

        Returns:
            Dict -- as is response from gmail api

        Reference:
            https://developers.google.com/gmail/api/v1/reference/users/messages/list
        """
        response = (
            self.service.users()
            .messages()
            .list(
                userId=self.user_id,
                q=search_query,
                labelIds=label_ids,
                includeSpamTrash=include_spam_trash,
                maxResults=results_per_page,
                pageToken=page_token,
            )
            .execute()
        )
        return response

    def _raw_gmail_message(self, message_id: str, return_format: str = "full") -> Dict:  # pragma: no cover
        """
        Get raw GMail `Users.messages` resource for the given message id. Example response format:
        https://developers.google.com/gmail/api/v1/reference/users/messages#resource

        Args:
          message_id: ID of message to get.
          format: The format to return the message in. Acceptable values are:
                "full": Returns the full email message data with body content parsed
                        in the payload field;the raw field is not used. (default).

                "metadata": Returns only email message ID, labels, and email headers.

                "minimal": Returns only email message ID and labels; does not return
                           the email headers, body, or payload.

                "raw": Returns the full email message data with body content in the raw
                       field as a base64url encoded string; the payload field is not used.

        Returns:
            Dict -- as is response from gmail api

        Reference:
            https://developers.google.com/gmail/api/v1/reference/users/messages/get
        """
        message = (
            self.service.users().messages().get(userId=self.user_id, id=message_id, format=return_format).execute()
        )

        return message

    def _raw_gmail_attachment(self, message_id: str, attachment_id: str) -> Dict:  # pragma: no cover
        """
        Get raw GMail ` Users.messages.attachments` resource for the given message id
        and attachment id. Example response format:
        https://developers.google.com/gmail/api/v1/reference/users/messages/attachments#resource

        Arguments:
            message_id {str} -- ID of message that the attachment belongs to.
            attachment_id {str} -- ID of attachment to get.

        Returns:
            Dict -- as is response from gmail api

        Reference:
            https://developers.google.com/gmail/api/v1/reference/users/messages/attachments/get
        """
        attachment = (
            self.service.users()
            .messages()
            .attachments()
            .get(userId=self.user_id, messageId=message_id, id=attachment_id)
            .execute()
        )
        return attachment

    # this is a bit useless at the moment and will need to be zapped at some point
    # def get_attachments_from_message(self, message: Dict, filetype_filter: str = None) -> Generator:
    #     """For a given gmail message response (non-raw), get all attachments

    #     Arguments:
    #         message {Dict} -- gmail api response for a message

    #     Keyword Arguments:
    #         filetype_filter {str} -- filtering filename (default: {None})

    #     Yields:
    #         Generator -- [description]
    #     """
    #     for part in message["payload"]["parts"]:
    #         if part["filename"] and (
    #             filetype_filter is None or filetype_filter.lower().endswith(filetype_filter.lower())
    #         ):
    #             attachment = self._raw_gmail_attachment(message["id"], part["body"]["attachmentId"])
    #             file_data = base64.urlsafe_b64decode(attachment["data"].encode("UTF-8"))

    #             # TODO: preserve file content as bytes. E.g. when a binary file format is attached [zip, xls, xlsx]
    #             yield File(part["filename"], BytesIO(file_data))

    @staticmethod
    def extract_message_content_from_body(message: dict) -> List[email.message.Message]:
        """
        Extract body content of raw email response.
        It attempts to strip out non useful stuff and bring back the html and txt of the email body

        Returns:
            List[email.message.Message] -- [description]
        """
        msg_str = base64.urlsafe_b64decode(message["raw"].encode("UTF-8"))
        mime_msg = email.message_from_bytes(msg_str)

        messageMainType = mime_msg.get_content_maintype()

        message_content = []

        if messageMainType == "multipart":
            for part in mime_msg.get_payload():
                if part.get_content_maintype() == "multipart":
                    message_content = part.get_payload()
        elif messageMainType == "text":
            message_content = mime_msg.get_payload()

        return message_content
