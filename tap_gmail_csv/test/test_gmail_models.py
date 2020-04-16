from unittest import TestCase
from unittest.mock import call, patch, MagicMock, mock_open
from io import BytesIO
from dataclasses import dataclass

from tap_gmail_csv.gmail_client.models import Attachment, File, Message, Url, FileNameCannotBeEvaluatedException
from tap_gmail_csv.gmail_client.client import GmailClient


class TestFile(TestCase):
    def setUp(self) -> None:
        self.file_name = "some_file.csv"
        self.raw_data = BytesIO(bytes("some data", "UTF-8"))
        self.file1 = File(self.file_name, self.raw_data)

    def test_file_constructor(self):
        # assert
        assert self.file1.file_name == self.file_name
        assert self.file1.raw_data == self.raw_data

    def test_file_equality(self):
        # setup
        file2 = File(self.file_name, self.raw_data)
        # assert
        assert self.file1 == self.file1
        assert self.file1 == file2

    def test_file_non_equality(self):
        # setup
        file2 = File("different.txt", self.raw_data)
        # assert
        assert self.file1 != file2

    def test_file_comparisons_against_other_types(self):
        # assert
        assert self.file1 != ""
        assert self.file1 != 0
        assert self.file1 is not True


class TestAttachment(TestCase):
    def setUp(self) -> None:
        self.message_id = "1"
        self.attachment_id = "2"
        self.attachment_name = "some_file.csv"
        self.attachment1 = Attachment(self.message_id, self.attachment_id, self.attachment_name)

    def test_attachment_constructor(self):
        # assert
        assert self.attachment1.message_id == self.message_id
        assert self.attachment1.attachment_id == self.attachment_id
        assert self.attachment1.attachment_name == self.attachment_name

    def test_attachment_equality(self):
        # setup
        attachment2 = Attachment(self.message_id, self.attachment_id, self.attachment_name)
        # assert
        assert self.attachment1 == self.attachment1
        assert self.attachment1 == attachment2

    def test_attachment_non_equality(self):
        # setup
        attachment2 = Attachment(self.message_id, self.attachment_id, "different.txt")
        # assert
        assert self.attachment1 != attachment2

    def test_attachment_comparisons_against_other_types(self):
        # assert
        assert self.attachment1 != ""
        assert self.attachment1 != 0
        assert self.attachment1 is not True

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._connect", MagicMock())
    @patch("base64.urlsafe_b64decode")
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._raw_gmail_attachment")
    def test_attachment_get_file(self, mock_raw_gmail_response, mock_b64decode):
        # setup
        client = GmailClient("auth_token_path", "user_id", "version")
        # mock
        mock_b64decode.return_value = bytes("some data", "UTF-8")
        # run
        file1 = self.attachment1.get_file(gmail_client=client)
        # assert
        mock_raw_gmail_response.assert_called_once_with(self.message_id, self.attachment_id)
        mock_b64decode.assert_called_once()
        self.assertIsInstance(file1, File)

    def test_attachment_get_file_raises_exception_without_client(self):
        # run and assert
        with self.assertRaises(TypeError):
            self.attachment1.get_file()


class TestMessage(TestCase):
    def setUp(self) -> None:

        self.message_id = "1"
        self.internal_date = 1000
        self.label_ids = ["Inbox"]
        self.attachment_list = [Attachment("1", "2", "file.txt")]
        self.url_list = ["https://download.me/file.csv"]
        self.email_to = "to.me@email.net"
        self.email_from = "from.you@email.net"
        self.email_subject = "subject"

        self.message1 = Message(
            self.message_id,
            self.internal_date,
            self.label_ids,
            self.attachment_list,
            self.url_list,
            self.email_to,
            self.email_from,
            self.email_subject,
        )

    def test_message_constructor(self):
        # assert
        assert self.message1.message_id == self.message_id
        assert self.message1.internal_date == self.internal_date
        assert self.message1.label_ids == self.label_ids
        assert self.message1.attachment_list == self.attachment_list
        assert self.message1.url_list == self.url_list
        assert self.message1.email_to == self.email_to
        assert self.message1.email_from == self.email_from
        assert self.message1.email_subject == self.email_subject

    def test_message_constructor_defaults(self):
        # setup
        message2 = Message(self.message_id, self.internal_date)
        # assert
        assert message2.message_id == self.message_id
        assert message2.internal_date == self.internal_date
        assert message2.label_ids is None
        assert message2.attachment_list is None
        assert message2.url_list is None
        assert message2.email_to is None
        assert message2.email_from is None
        assert message2.email_subject is None

    def test_message_equality(self):
        # setup
        message2 = Message(
            self.message_id,
            self.internal_date,
            self.label_ids,
            self.attachment_list,
            self.url_list,
            self.email_to,
            self.email_from,
            self.email_subject,
        )
        # assert
        assert self.message1 == self.message1
        assert self.message1 == message2

    def test_message_non_equality(self):
        # setup
        message2 = Message("1", 2000)
        # assert
        assert self.message1 != message2

    def test_message_comparisons_against_other_types(self):
        # assert
        assert self.message1 != ""
        assert self.message1 != 0
        assert self.message1 is not True

    def test_message_lt(self):
        # setup
        message0 = Message("0", 100)
        message1 = Message("1", 200)
        # assert
        assert message0 < message1
        assert not message1 < message0

    def test_message_gt(self):
        # setup
        message0 = Message("0", 100)
        message1 = Message("1", 200)
        # assert
        assert message1 > message0
        assert not message0 > message1

    @patch("tap_gmail_csv.gmail_client.models.Message._filter_url_list")
    @patch("tap_gmail_csv.gmail_client.models.Message._filter_attachment_list")
    def test_filter(self, mock_filter_attachments, mock_filter_urls):
        # setup
        filter_pattern = "some regex"
        # run
        self.message1.filter(filter_pattern)
        # assert
        mock_filter_attachments.assert_called_once_with(filter_pattern)
        mock_filter_urls.assert_called_once_with(filter_pattern)

    def test_filter_attachment_list(self):
        # setup
        self.message1.attachment_list = [
            Attachment("1", "2", "file.txt"),
            Attachment("1", "2", "file.csv"),
            Attachment("1", "2", "file.xls"),
        ]
        filter_pattern = "(.*)\\.csv$"
        # run
        self.message1._filter_attachment_list(filter_pattern)
        # assert
        assert self.message1.attachment_list == [Attachment("1", "2", "file.csv")]

    def test_filter_attachment_list_none_check(self):
        # setup
        self.message1.attachment_list = None
        filter_pattern = "(.*)\\.csv$"
        # run
        self.message1._filter_attachment_list(filter_pattern)
        # assert
        assert self.message1.attachment_list is None

    def test_filter_url_list(self):
        # setup
        self.message1.url_list = [
            Url("1", "https://www.some.sebsite.com/reports-for-you/file.txt"),
            Url("1", "https://www.some.sebsite.com/reports-for-you/file.csv"),
            Url("1", "https://www.some.sebsite.com/reports-for-you/file.xls"),
        ]
        filter_pattern = "(.*)\\.csv$"
        # run
        self.message1._filter_url_list(filter_pattern)
        # assert
        assert self.message1.url_list == [Url("1", "https://www.some.sebsite.com/reports-for-you/file.csv")]

    def test_filter_url_list_none_check(self):
        # setup
        self.message1.url_list = None
        filter_pattern = "(.*)\\.csv$"
        # run
        self.message1._filter_url_list(filter_pattern)
        # assert
        assert self.message1.url_list is None


class TestUrl(TestCase):
    def setUp(self) -> None:
        self.message_id = "1"
        self.url = "http://www.some.url.com/file.csv"
        self.url1 = Url(self.message_id, self.url)

    def test_url_constructor(self):
        # assert
        assert self.url1.message_id == self.message_id
        assert self.url1.url == self.url

    def test_url_equality(self):
        # setup
        url2 = Url(self.message_id, self.url)
        # assert
        assert self.url1 == self.url1
        assert self.url1 == url2

    def test_url_non_equality(self):
        # setup
        url2 = Url(self.message_id, "http://www.another.url.com/different.txt")
        # assert
        assert self.url1 != url2

    def test_url_comparisons_against_other_types(self):
        # assert
        assert self.url1 != ""
        assert self.url1 != 0
        assert self.url1 is not True

    @patch("tap_gmail_csv.gmail_client.models.Url._download_from_url")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_file_name")
    def test_get_file(self, mock_get_file_name, mock_download):
        # mock
        mock_get_file_name.return_value = "test.csv"
        mock_download.return_value = b"Some,Data"
        # setup
        expected = File("test.csv", BytesIO(b"Some,Data"))
        # run
        actual = self.url1.get_file()
        # assert
        mock_get_file_name.assert_called_once()
        mock_download.assert_called_once()
        assert expected == actual

    @patch("tap_gmail_csv.gmail_client.models.requests.head")
    def test_get_url_http_headers(self, mock_requests_head):
        # setup
        expected = {"content-type": "text/csv"}

        @dataclass
        class RequestsResponse:
            headers: dict

        # mock
        mock_requests_head.return_value = RequestsResponse(headers=expected)
        # run
        actual = self.url1._get_url_http_headers()
        # assert
        mock_requests_head.assert_called_once_with(self.url1.url, allow_redirects=True)
        assert expected == actual

    def test_check_url_file_type(self):
        # setup
        # tuple of (test, expected)
        tests = [
            ("text/csv", "csv"),
            ("text/plain", "csv"),
            ("application/excel", "xls"),
            ("application/vnd.ms-excel", "xls"),
            ("application/x-excel", "xls"),
            ("application/x-msexcel", "xls"),
            ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "xlsx"),
            ("application/zip", "zip"),
            ("application/x-compressed", "zip"),
            ("application/x-zip-compressed", "zip"),
            ("multipart/x-zip", "zip"),
            ("application/json", None),
            ("", None),
        ]
        # run and assert
        for t in tests:
            actual = Url._check_url_file_type({"content-type": t[0] + " some other stuffs"})
            assert actual == t[1]

    def test_check_url_file_type_for_unsupported_types(self):
        # setup - tuple of (test, expected)
        tests = [
            ("application/json", None),
            ("", None),
        ]
        # run
        for t in tests:
            actual = Url._check_url_file_type({"content-type": t[0] + " some other stuffs"})
            expected = t[1]
            # assert
            assert actual == expected

    def test_get_filename_from_headers(self):
        # setup - tuple of (test, expected)
        tests = [
            ("filename=file.pdf", "file.pdf"),
            ("attachment; filename=file.pdf;", "file.pdf"),
            ("attachment; filename=file.pdf  ;", "file.pdf"),
            ("attachment; filename=file.pdf; form-data;", "file.pdf"),
        ]
        # run
        for t in tests:
            actual = Url._get_filename_from_headers({"content-disposition": t[0]})
            expected = t[1]
            # assert
            assert actual == expected

    def test_get_filename_from_headers_none_conditions(self):
        # setup
        tests = [
            ("file.pdf", None),
            ("filename=", None),
            ("attachment; file=file.pdf;", None),
            ("attachment;", None),
            ("", None),
            (None, None),
        ]
        # run
        for t in tests:
            actual = Url._get_filename_from_headers({"content-disposition": t[0]})
            expected = t[1]
            # assert
            assert actual == expected

    def test_get_filename_from_url(self):
        # setup
        tests = [
            ("https://www.some.sebsite.com/reports-for-you/file.csv", "file.csv"),
            ("https://www.some.sebsite.com/reports-for-you/file", "file"),
            ("https://www.some.sebsite.com/reports-for-you/file.csv?_utm_source=abc&_utm_campaign=xyz", "file.csv"),
            ("/file.csv", "file.csv"),
        ]
        # run
        for t in tests:
            test_url = Url("1", t[0])
            actual = test_url._get_filename_from_url()
            expected = t[1]
            # assert
            assert actual == expected

    def test_get_filename_from_url_none_conditions(self):
        # setup
        tests = [
            ("file.csv", None),
            ("www.text.com/file.csv/", None),
        ]
        # run
        for t in tests:
            test_url = Url("1", t[0])
            actual = test_url._get_filename_from_url()
            expected = t[1]
            # assert
            assert actual == expected

    def test_add_file_extension(self):
        # setup
        tests = [
            ("", "csv", ".csv"),
            ("f", "csv", "f.csv"),
            ("file", "csv", "file.csv"),
            ("file", "xlsx", "file.xlsx"),
            ("file.csv", "csv", "file.csv"),
            ("file.txt", "csv", "file.txt.csv"),
        ]
        # run
        for t in tests:
            actual = Url._add_file_extension(t[0], t[1])
            expected = t[2]
            # assert
            assert actual == expected

    @patch("tap_gmail_csv.gmail_client.models.Url._get_url_http_headers", MagicMock())
    @patch("tap_gmail_csv.gmail_client.models.Url._add_file_extension")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_filename_from_url")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_filename_from_headers")
    @patch("tap_gmail_csv.gmail_client.models.Url._check_url_file_type")
    def test_get_file_name_found_at_header_level(
        self, mock_check_filetype, mock_filename_from_headers, mock_filename_from_url, mock_file_extension
    ):
        # setup
        expected = "file.csv"
        # mock
        mock_check_filetype.return_value = "csv"
        mock_filename_from_headers.return_value = expected
        mock_file_extension.return_value = expected
        # run
        actual = self.url1._get_file_name()
        # assert
        mock_filename_from_url.assert_not_called()
        mock_file_extension.assert_called_once_with("file.csv", "csv")
        assert actual == expected

    @patch("tap_gmail_csv.gmail_client.models.Url._get_url_http_headers", MagicMock())
    @patch("tap_gmail_csv.gmail_client.models.Url._add_file_extension")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_filename_from_url")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_filename_from_headers")
    @patch("tap_gmail_csv.gmail_client.models.Url._check_url_file_type")
    def test_get_file_name_found_at_url_level(
        self, mock_check_filetype, mock_filename_from_headers, mock_filename_from_url, mock_file_extension
    ):
        # setup
        expected = "file.csv"
        # mock
        mock_check_filetype.return_value = "csv"
        mock_filename_from_headers.return_value = None
        mock_filename_from_url.return_value = expected
        mock_file_extension.return_value = expected
        # run
        actual = self.url1._get_file_name()
        # assert
        mock_file_extension.assert_called_once_with("file.csv", "csv")
        assert actual == expected

    @patch("tap_gmail_csv.gmail_client.models.Url._get_url_http_headers", MagicMock())
    @patch("tap_gmail_csv.gmail_client.models.Url._add_file_extension")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_filename_from_url")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_filename_from_headers")
    @patch("tap_gmail_csv.gmail_client.models.Url._check_url_file_type")
    def test_get_file_name_found_and_unidentified_filetype(
        self, mock_check_filetype, mock_filename_from_headers, mock_filename_from_url, mock_file_extension
    ):
        # setup
        expected = "file.csv"
        # mock
        mock_check_filetype.return_value = None
        mock_filename_from_headers.return_value = None
        mock_filename_from_url.return_value = expected
        mock_file_extension.return_value = expected
        # run
        actual = self.url1._get_file_name()
        # assert
        mock_file_extension.assert_not_called()
        assert actual == expected

    @patch("tap_gmail_csv.gmail_client.models.Url._get_url_http_headers", MagicMock())
    @patch("tap_gmail_csv.gmail_client.models.Url._add_file_extension")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_filename_from_url")
    @patch("tap_gmail_csv.gmail_client.models.Url._get_filename_from_headers")
    @patch("tap_gmail_csv.gmail_client.models.Url._check_url_file_type")
    def test_get_file_name_raises_exception_when_url_cannot_be_evaluated_bcuz_filename(
        self, mock_check_filetype, mock_filename_from_headers, mock_filename_from_url, mock_file_extension
    ):
        # setup
        expected = "file.csv"
        # mock
        mock_check_filetype.return_value = "csv"
        mock_filename_from_headers.return_value = None
        mock_filename_from_url.return_value = None
        # run assert
        with self.assertRaises(FileNameCannotBeEvaluatedException):
            self.url1._get_file_name()
        mock_file_extension.assert_not_called()

    @patch("tap_gmail_csv.gmail_client.models.requests.get")
    def test_download_from_url(self, mock_requests_get):
        # setup
        expected = b"content"

        @dataclass
        class RequestsResponse:
            content: dict

        # mock
        mock_requests_get.return_value = RequestsResponse(content=expected)
        # run
        actual = self.url1._download_from_url()
        # assert
        mock_requests_get.assert_called_once_with(self.url1.url, allow_redirects=True)
        assert expected == actual
