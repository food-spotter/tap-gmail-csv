from unittest import TestCase
from unittest.mock import call, patch, MagicMock, mock_open

from io import BytesIO

from tap_gmail_csv.gmail_client.models import Attachment, File, Message
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
        assert self.file1 != True


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
        assert self.attachment1 != True

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._connect", MagicMock())
    @patch("base64.urlsafe_b64decode")
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._raw_gmail_attachment")
    def test_attachment_get_file(self, mock_raw_gmail_response, mock_b64decode):
        # setup
        client = GmailClient("auth_token_path", "user_id", "version")
        # mock
        mock_b64decode.return_value = bytes("some data", "UTF-8")
        # run
        file1 = self.attachment1.get_file(client)
        # assert
        mock_raw_gmail_response.assert_called_once_with(self.message_id, self.attachment_id)
        mock_b64decode.assert_called_once()
        self.assertIsInstance(file1, File)


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
        assert self.message1 != True

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
            "https://www.some.sebsite.com/reports-for-you/file.txt",
            "https://www.some.sebsite.com/reports-for-you/file.csv",
            "https://www.some.sebsite.com/reports-for-you/file.xls",
        ]
        filter_pattern = "(.*)\\.csv$"
        # run
        self.message1._filter_url_list(filter_pattern)
        # assert
        assert self.message1.url_list == ["https://www.some.sebsite.com/reports-for-you/file.csv"]

    def test_filter_url_list_none_check(self):
        # setup
        self.message1.url_list = None
        filter_pattern = "(.*)\\.csv$"
        # run
        self.message1._filter_url_list(filter_pattern)
        # assert
        assert self.message1.url_list is None
