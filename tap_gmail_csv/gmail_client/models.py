import base64
import re
from io import BytesIO
from dataclasses import dataclass
from typing import List, Optional, Pattern


# to get round circular dependency - eww
# class Message:
#     pass


@dataclass
class File:
    """
    Represents a File
    """

    file_name: str
    raw_data: BytesIO

    def __eq__(self, other):
        if other.__class__ is not self.__class__:
            return NotImplemented
        return (self.file_name, self.raw_data) == (other.file_name, other.raw_data)


@dataclass
class Attachment:
    """
    Represents a GMail Attachment.
    """

    message_id: str
    attachment_id: str
    attachment_name: str

    def __eq__(self, other):
        if other.__class__ is not self.__class__:
            return NotImplemented
        return (self.message_id, self.attachment_id, self.attachment_name) == (
            other.message_id,
            other.attachment_id,
            other.attachment_name,
        )

    def get_file(self, gmail_client) -> File:
        attachment = gmail_client._raw_gmail_attachment(self.message_id, self.attachment_id)
        file_data = base64.urlsafe_b64decode(attachment["data"].encode("UTF-8"))
        return File(self.attachment_name, BytesIO(file_data))


@dataclass
class Message:
    """
    Represents a simplified GMail Message with the juicy bits.
    """

    message_id: str
    internal_date: int
    label_ids: Optional[List[str]] = None
    attachment_list: Optional[List[Attachment]] = None
    download_urls: Optional[List[str]] = None
    email_to: Optional[str] = None
    email_from: Optional[str] = None
    email_subject: Optional[str] = None

    def __eq__(self, other):
        if other.__class__ is not self.__class__:
            return NotImplemented
        return (
            self.message_id,
            self.internal_date,
            self.label_ids,
            self.attachment_list,
            self.download_urls,
            self.email_to,
            self.email_from,
            self.email_subject,
        ) == (
            other.message_id,
            other.internal_date,
            other.label_ids,
            other.attachment_list,
            other.download_urls,
            other.email_to,
            other.email_from,
            other.email_subject,
        )

    def __lt__(self, other):
        return self.internal_date < other.internal_date

    def __gt__(self, other):
        return self.internal_date > other.internal_date

    def filter(self, regex_pattern: str) -> None:
        """
        Apply regex filter to filenames in `attachment_list` and urls in `download_urls`.

        Arguments:
            regex_pattern {str}
        """
        self._filter_attachment_list(regex_pattern)
        self._filter_download_urls(regex_pattern)

    def _filter_attachment_list(self, regex_pattern: str) -> None:
        """
        Apply regex filter on each `Attachment.attachment_name` within `self.attachment_list`

        Arguments:
            regex_pattern {str}
        """
        matcher = re.compile(regex_pattern)
        filtered_list = []
        if self.attachment_list:
            for attachment in self.attachment_list:
                if matcher.search(attachment.attachment_name):
                    filtered_list.append(attachment)
            self.attachment_list = filtered_list

    def _filter_download_urls(self, regex_pattern: str) -> None:
        """
        Apply regex filter on each download url string within `self.download_urls`

        Arguments:
            regex_pattern {str}
        """
        matcher = re.compile(regex_pattern)
        filtered_list = []
        if self.download_urls:
            for url in self.download_urls:
                if matcher.search(url):
                    filtered_list.append(url)
            self.download_urls = filtered_list
