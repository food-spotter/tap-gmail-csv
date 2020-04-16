import base64
import re
from io import BytesIO
from dataclasses import dataclass
from typing import List, Optional, Dict

import requests


# to get round circular dependency - eww
# class Message:
#     pass


SUPPORTED_MIME_TYPES = {
    "csv": ["text/csv", "text/plain"],
    "xls": ["application/excel", "application/vnd.ms-excel", "application/x-excel", "application/x-msexcel"],
    "xlsx": ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"],
    "zip": ["application/zip", "application/x-compressed", "application/x-zip-compressed", "multipart/x-zip"],
}


class FileNameCannotBeEvaluatedException(Exception):
    pass


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
        return (self.file_name, self.raw_data.getvalue()) == (other.file_name, other.raw_data.getvalue())


@dataclass
class BasicMessageResource:
    """
    Represents a basic resource that belongs to a Message.
    """

    message_id: str

    def get_file(self, **kwargs) -> File:
        pass


@dataclass
class Attachment(BasicMessageResource):
    """
    Represents a GMail Attachment.
    """

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

    def get_file(self, gmail_client=None, **kwargs) -> File:
        """
        Fetches raw data from the attached file and returns a File object

        Keyword Arguments:
            gmail_client {GmailClient}

        Raises:
            TypeError: if invalid `gmail_client`

        Returns:
            File -- [description]
        """
        if gmail_client is None:
            raise TypeError("Required keyword argument: gmail_client must not be None.")

        attachment = gmail_client._raw_gmail_attachment(self.message_id, self.attachment_id)
        file_data = base64.urlsafe_b64decode(attachment["data"].encode("UTF-8"))
        return File(self.attachment_name, BytesIO(file_data))


@dataclass
class Url(BasicMessageResource):
    """
    Represents a URL found in a GMail Message.
    """

    url: str

    def __eq__(self, other):
        if other.__class__ is not self.__class__:
            return NotImplemented
        return (self.message_id, self.url) == (other.message_id, other.url,)

    def get_file(self, **kwargs) -> File:
        """
        Fetches raw data from the attached file and returns a File object

        Returns:
            File
        """
        file_name = self._get_file_name()
        file_data = self._download_from_url()
        return File(file_name, BytesIO(file_data))

    def _get_url_http_headers(self) -> Dict[str, str]:
        """
        Get HTTP headers for the given URL.

        Returns:
            Dict[str, str] -- headers data
        """
        h = requests.head(self.url, allow_redirects=True)
        return h.headers

    @staticmethod
    def _check_url_file_type(headers: Dict[str, str]) -> Optional[str]:
        """
        Returns the file type from a whitelisted set of mime types.

        This is done by fetching the header and checking `SUPPORTED_MIME_TYPES`
        against the `content-type` header. If it is an unsupported type, returns `None`

        Since this uses `requests.head`, no download is done.

        Returns:
            Optional[str] -- "csv" | "xls" | "xlsx" | "zip" | None
        """
        content_type = headers.get("content-type", "").lower()
        file_type = None

        for extension in SUPPORTED_MIME_TYPES.keys():
            for mime_type in SUPPORTED_MIME_TYPES.get(extension, []):
                if mime_type in content_type:
                    file_type = extension
                    break

        return file_type

    @staticmethod
    def _get_filename_from_headers(headers: Dict[str, str]) -> Optional[str]:
        """
        Get filename from `content-disposition` in the headers.

        Arguments:
            headers {Dict[str, str]} -- header from a `requests` response

        Returns:
            Optional[str] -- The filename found in the headers. `None` if not found.
        """
        content_disposition = headers.get("content-disposition", None)
        if content_disposition:
            for chunk in content_disposition.split(";"):
                file_name = re.findall("filename=(.+)", chunk)
                if len(file_name) > 0:
                    return file_name[0].strip()

        return None

    def _get_filename_from_url(self) -> Optional[str]:
        """
        Extract filename from the URL.

        Example:
        `https://file-examples.com/wp-content/uploads/2017/02/file.csv?_utm_source=abc123`
        will return `file.csv`

        Returns:
            Optional[str] -- file name. `None` if the file_name could not be deduced from the URL
        """
        file_name_portion = None

        right_portion = self.url.rsplit("/", 1)
        if len(right_portion) == 2:
            # split any potential query params - these start with "?""
            file_name_portion = right_portion[1].split("?")[0].strip()

            if len(file_name_portion) == 0:
                file_name_portion = None

        return file_name_portion

    @staticmethod
    def _add_file_extension(file_name: str, extension: str) -> str:
        """
        Adds the file extension if it's missing. Returns as is if already present.

        Arguments:
            file_name {str}
            extension {str} -- file extension. e.g. "csv", "xlsx", etc...

        Returns:
            str -- filename with extension added
        """
        fname = file_name.strip()
        slice_offset = -1 * (len(extension) + 1)
        if fname[slice_offset:] != f".{extension}":
            fname = fname + f".{extension}"
        return fname

    def _get_file_name(self) -> str:
        """
        Get filename from the URL. First checks the `content-disposition` in the http headers for
        the correct file name. If not found, use the right most value in the URL.
        """
        headers = self._get_url_http_headers()
        file_type = self._check_url_file_type(headers)
        file_name = self._get_filename_from_headers(headers)

        if not file_name:
            file_name = self._get_filename_from_url()

        if file_name is None:
            raise FileNameCannotBeEvaluatedException

        if file_type:
            file_name = self._add_file_extension(file_name, file_type)

        return file_name

    def _download_from_url(self) -> bytes:
        """
        Download the URL content

        Returns:
            bytes
        """
        response = requests.get(self.url, allow_redirects=True)
        return response.content


@dataclass
class Message:
    """
    Represents a simplified GMail Message with the juicy bits.
    """

    message_id: str
    internal_date: int
    label_ids: Optional[List[str]] = None
    attachment_list: Optional[List[Attachment]] = None
    url_list: Optional[List[Url]] = None
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
            self.url_list,
            self.email_to,
            self.email_from,
            self.email_subject,
        ) == (
            other.message_id,
            other.internal_date,
            other.label_ids,
            other.attachment_list,
            other.url_list,
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
        Apply regex filter to filenames in `attachment_list` and urls in `url_list`.

        Arguments:
            regex_pattern {str}
        """
        self._filter_attachment_list(regex_pattern)
        self._filter_url_list(regex_pattern)

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

    def _filter_url_list(self, regex_pattern: str) -> None:
        """
        Apply regex filter on each Url object within `self.url_list`

        Arguments:
            regex_pattern {str}
        """
        matcher = re.compile(regex_pattern)
        filtered_list = []
        if self.url_list:
            for url in self.url_list:
                if matcher.search(url.url):
                    filtered_list.append(url)
            self.url_list = filtered_list
