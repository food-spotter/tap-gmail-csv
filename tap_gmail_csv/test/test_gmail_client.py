from unittest import TestCase
from unittest.mock import call, patch, MagicMock, mock_open
from tap_gmail_csv.gmail_client.client import (
    GmailClient,
    GoogleAPICredentialsAreAnInvalidFormat,
    GoogleAPICredentialsNotFound,
)
from tap_gmail_csv.gmail_client.models import Attachment, Message, Url


class TestGmailClient(TestCase):
    def setUp(self) -> None:
        self.sample_gmail_response = {
            "historyId": "55390",
            "id": "17092f63a2963a4f",
            "internalDate": "1583013578000",
            "labelIds": ["UNREAD", "SENT", "INBOX"],
            "payload": {
                "body": {"size": 0},
                "filename": "",
                "headers": [
                    {"name": "MIME-Version", "value": "1.0"},
                    {"name": "Date", "value": "Sat, 29 Feb 2020 21:59:38 +0000"},
                    {"name": "Message-ID", "value": "<CAJcrew==abc123xyz@mail.gmail.com>"},
                    {"name": "Subject", "value": "test the csv reading capabilities"},
                    {"name": "From", "value": "Some Sender <some.sender@gmail.com>"},
                    {"name": "To", "value": "to.me+test@gmail.com"},
                    {"name": "Content-Type", "value": 'multipart/mixed; boundary="0123456abcXYZ"'},
                ],
                "mimeType": "multipart/mixed",
                "partId": "",
                "parts": [
                    {
                        "body": {"size": 0},
                        "filename": "",
                        "headers": [{"name": "Content-Type", "value": 'multipart/alternative; boundary="78910abcXYZ"'}],
                        "mimeType": "multipart/alternative",
                        "partId": "0",
                        "parts": [
                            {
                                "body": {"data": "0123456abcXYZ==", "size": 43},
                                "filename": "",
                                "headers": [{"name": "Content-Type", "value": 'text/plain; charset="UTF-8"'}],
                                "mimeType": "text/plain",
                                "partId": "0.0",
                            },
                            {
                                "body": {"data": "0123456abcXYZ", "size": 90},
                                "filename": "",
                                "headers": [{"name": "Content-Type", "value": 'text/html; charset="UTF-8"'}],
                                "mimeType": "text/html",
                                "partId": "0.1",
                            },
                        ],
                    },
                    {
                        "partId": "1",
                        "mimeType": "text/html",
                        "filename": "",
                        "headers": [
                            {"name": "Content-Type", "value": 'text/html; charset="UTF-8"'},
                            {"name": "Content-Transfer-Encoding", "value": "quoted-printable"},
                        ],
                        "body": {"size": 4602, "data": "base64encodedHtmlData"},
                    },
                    {
                        "body": {"attachmentId": "0123456abcXYZ-attachment", "size": 6051},
                        "filename": "MOCK_DATA.csv",
                        "headers": [
                            {"name": "Content-Type", "value": 'application/vnd.ms-excel; name="MOCK_DATA.csv"'},
                            {"name": "Content-Disposition", "value": 'attachment; filename="MOCK_DATA.csv"'},
                            {"name": "Content-Transfer-Encoding", "value": "base64"},
                            {"name": "X-Attachment-Id", "value": "f_k785ejfu0"},
                            {"name": "Content-ID", "value": "<f_k785ejfu0>"},
                        ],
                        "mimeType": "application/vnd.ms-excel",
                        "partId": "2",
                    },
                ],
            },
            "sizeEstimate": 9336,
            "snippet": "Mock data has been attached to this email",
            "threadId": "17092f5508289c87",
        }

    @patch("tap_gmail_csv.gmail_client.client.build")
    def test_connect_successfully(self, mock_build):
        # setup
        creds = MagicMock()
        api_version = "v3"
        # run
        test = GmailClient._create_client(creds, api_version, cache_discovery=False)
        # assert
        mock_build.assert_called_once_with("gmail", api_version, cache_discovery=False, credentials=creds)

    @patch("builtins.open", new_callable=mock_open, read_data="some token")
    @patch("pickle.load")
    @patch("os.path.exists")
    def test_get_credentials_success(self, mock_path_exists, mock_pickle_load, mock_file):
        # setup
        file_path = "some/path/token.pickle"
        mock_path_exists.return_value = True
        mock_pickle_load.return_value = "some credentials"
        # run
        creds = GmailClient._get_credentials(file_path)
        # assert
        mock_file.assert_called_with(file_path, "rb")
        mock_pickle_load.assert_called_once()
        self.assertEqual(creds, "some credentials")

    @patch("os.path.exists")
    def test_get_credentials_raises_credential_not_found_exception(self, mock_path_exists):
        # setup
        mock_path_exists.return_value = False
        # run & assert
        with self.assertRaises(GoogleAPICredentialsNotFound):
            GmailClient._get_credentials("some/invalid/path")

    @patch("builtins.open", new_callable=mock_open, read_data="some token")
    @patch("pickle.load")
    @patch("os.path.exists")
    def test_get_credentials_raises_credential_are_an_invalid_format_exception(
        self, mock_path_exists, mock_pickle_load, mock_file
    ):
        # setup
        file_path = "some/path/token.pickle"
        mock_path_exists.return_value = True
        mock_pickle_load.side_effect = GoogleAPICredentialsAreAnInvalidFormat()
        # run & assert
        with self.assertRaises(GoogleAPICredentialsAreAnInvalidFormat):
            GmailClient._get_credentials(file_path)

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._get_credentials")
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._create_client")
    def test_connect(self, mock_create_client, mock_get_creds):
        # setup
        auth_token_path = "auth.token/path"
        version = "v3"
        mock_get_creds.return_value = "some credentials"
        # run
        GmailClient._connect(auth_token_path, version)
        # assert
        mock_get_creds.assert_called_once_with(auth_token_path)
        mock_create_client.assert_called_once_with("some credentials", version)

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._connect")
    def test_init_google_client(self, mock_connect):
        # setup
        auth_token_path = "auth.token/path"
        user_id = "some@gmail.com"
        version = "v3"
        mock_connect.return_value = "some gmail api object"
        # run
        client = GmailClient(auth_token_path, user_id, version)
        # assert
        mock_connect.assert_called_once_with(auth_token_path, version)
        self.assertEqual(client.service, "some gmail api object")
        self.assertEqual(client.user_id, user_id)

    def test_convert_to_attachment_with_attachment(self):
        # setup
        test_part = self.sample_gmail_response.get("payload").get("parts")[2]
        message_id = self.sample_gmail_response.get("id")
        expected = Attachment(message_id, "0123456abcXYZ-attachment", "MOCK_DATA.csv")
        # run
        actual = GmailClient._convert_to_attachment(message_id, test_part)
        # assert
        assert expected == actual

    def test_convert_to_attachment_with_no_attachment(self):
        # setup
        test_part = self.sample_gmail_response.get("payload").get("parts")[0]
        message_id = self.sample_gmail_response.get("id")
        expected = None
        # run
        actual = GmailClient._convert_to_attachment(message_id, test_part)
        # assert
        assert expected == actual

    def test_convert_to_attachment_list_with_attachment(self):
        # setup
        message = self.sample_gmail_response
        message_id = self.sample_gmail_response.get("id")
        expected = [Attachment(message_id, "0123456abcXYZ-attachment", "MOCK_DATA.csv")]
        # run
        actual = GmailClient._convert_to_attachment_list(message)
        # assert
        assert expected == actual

    def test_convert_to_attachment_list_without_attachment(self):
        # setup
        message = self.sample_gmail_response
        message.get("payload").get("parts").pop()
        message_id = self.sample_gmail_response.get("id")
        expected = []
        # run
        actual = GmailClient._convert_to_attachment_list(message)
        # assert
        assert expected == actual

    def test_find_in_header_no_payload(self):
        # setup
        message = {}
        key = "subject"
        expected = None
        # run
        actual = GmailClient._find_in_header(message, key)
        # assert
        assert expected == actual

    def test_find_in_header_no_headers(self):
        # setup
        message = {"payload": {}}
        key = "subject"
        expected = None
        # run
        actual = GmailClient._find_in_header(message, key)
        # assert
        assert expected == actual

    def test_find_in_header_case_insensitive(self):
        # setup
        message = self.sample_gmail_response
        key = "to"
        expected = "to.me+test@gmail.com"
        # run
        actual = GmailClient._find_in_header(message, key)
        # assert
        assert expected == actual

    def test_find_in_header_mo_match_found(self):
        # setup
        message = self.sample_gmail_response
        key = "x-token"
        expected = None
        # run
        actual = GmailClient._find_in_header(message, key)
        # assert
        assert expected == actual

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._find_in_header")
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._convert_to_url_list")
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._convert_to_attachment_list")
    def test_convert_to_message_obj(self, mock_convert_to_attachment_list, mock_convert_to_url_list, mock_find_header):
        # setup
        message = self.sample_gmail_response
        # mock
        mock_convert_to_attachment_list.return_value = [Attachment("1", "2", "file.csv")]
        mock_convert_to_url_list.return_value = []
        mock_find_header.side_effect = ["to_x", "from_x", "subject_x"]

        expected = Message(
            message.get("id"),
            message.get("internalDate"),
            message.get("labelIds"),
            [Attachment("1", "2", "file.csv")],
            [],
            "to_x",
            "from_x",
            "subject_x",
        )
        # run
        actual = GmailClient._convert_to_message_obj(message)
        # assert
        mock_convert_to_attachment_list.assert_called_once_with(message)
        mock_find_header.assert_has_calls([call(message, "To"), call(message, "From"), call(message, "Subject")])
        assert expected == actual

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._connect", MagicMock())
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._convert_to_message_obj")
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._raw_gmail_message")
    def test_get_messages_attachment_mode(self, mock_raw_gmail_message, mock_convert_to_message):
        # setup
        raw_message_list = [
            {"id": "100", "threadId": "2715e11441a6d424"},
            {"id": "101", "threadId": "2715e11441a6d424"},
            {"id": "102", "threadId": "2715e11441a6d424"},
        ]
        expected = [Message("100", 100), Message("101", 101), Message("102", 102)]
        # mock
        mock_raw_gmail_message.side_effect = [{"1": "1"}, {"2": "2"}, {"3": "3"}]
        mock_convert_to_message.side_effect = expected
        # run
        client = GmailClient("auth_token_path", "user_id", "version")
        actual = client._get_messages(raw_message_list)
        # assert
        assert expected == list(actual)
        # after yielding all results, we can validate the calls
        mock_raw_gmail_message.assert_has_calls([call("100"), call("101"), call("102")])
        mock_convert_to_message.assert_has_calls([call({"1": "1"}), call({"2": "2"}), call({"3": "3"})])

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._connect", MagicMock())
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._get_messages")
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._raw_search_gmail")
    def test_search(self, mock_raw_search_gmail, mock_get_messages):
        # setup
        search_query = "from: me"
        label_ids = ["INBOX"]
        include_spam_trash = False
        results_per_page = 100
        max_search_results = 100
        # mock
        mock_raw_search_gmail.return_value = [1, 2, 3, 4]
        # run
        client = GmailClient("auth_token_path", "user_id", "version")
        client.search(search_query, label_ids, include_spam_trash, results_per_page, max_search_results)
        # assert
        mock_raw_search_gmail.assert_called_once_with(
            search_query, label_ids, include_spam_trash, results_per_page, max_search_results
        )
        mock_get_messages.assert_called_once_with([1, 2, 3, 4])

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._connect", MagicMock())
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._raw_gmail_list_messages")
    def test_raw_search_gmail_multiple_iterations(self, mock_gmail_response):
        # mock
        mock_gmail_response.side_effect = [
            {
                "messages": [
                    {"id": "101", "threadId": "2715e11441a6d424"},
                    {"id": "102", "threadId": "2715e11441a6d424"},
                    {"id": "103", "threadId": "2715e11441a6d424"},
                    {"id": "104", "threadId": "2715e11441a6d424"},
                    {"id": "105", "threadId": "2715e11441a6d424"},
                ],
                "nextPageToken": "some_token",
                "resultSizeEstimate": 5,
            },
            {
                "messages": [
                    {"id": "106", "threadId": "2715e11441a6d424"},
                    {"id": "107", "threadId": "2715e11441a6d424"},
                    {"id": "108", "threadId": "2715e11441a6d424"},
                    {"id": "109", "threadId": "2715e11441a6d424"},
                    {"id": "110", "threadId": "2715e11441a6d424"},
                ],
                "resultSizeEstimate": 2,
            },
        ]
        # setup
        search_query = "from: me"
        label_ids = ["INBOX"]
        include_spam_trash = False
        results_per_page = 5
        max_search_results = 7
        expected = [
            {"id": "101", "threadId": "2715e11441a6d424"},
            {"id": "102", "threadId": "2715e11441a6d424"},
            {"id": "103", "threadId": "2715e11441a6d424"},
            {"id": "104", "threadId": "2715e11441a6d424"},
            {"id": "105", "threadId": "2715e11441a6d424"},
            {"id": "106", "threadId": "2715e11441a6d424"},
            {"id": "107", "threadId": "2715e11441a6d424"},
        ]
        # run
        client = GmailClient("auth_token_path", "user_id", "version")
        actual = client._raw_search_gmail(
            search_query, label_ids, include_spam_trash, results_per_page, max_search_results
        )
        # assert
        assert expected == list(actual)

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._connect", MagicMock())
    @patch("tap_gmail_csv.gmail_client.client.GmailClient._raw_gmail_list_messages")
    def test_raw_search_gmail_single_iteration(self, mock_gmail_response):
        # mock
        mock_gmail_response.side_effect = [
            {
                "messages": [
                    {"id": "101", "threadId": "2715e11441a6d424"},
                    {"id": "102", "threadId": "2715e11441a6d424"},
                    {"id": "103", "threadId": "2715e11441a6d424"},
                    {"id": "104", "threadId": "2715e11441a6d424"},
                    {"id": "105", "threadId": "2715e11441a6d424"},
                ],
                "resultSizeEstimate": 5,
            }
        ]
        # setup
        search_query = "from: me"
        label_ids = ["INBOX"]
        include_spam_trash = False
        results_per_page = 5
        max_search_results = 3
        expected = [
            {"id": "101", "threadId": "2715e11441a6d424"},
            {"id": "102", "threadId": "2715e11441a6d424"},
            {"id": "103", "threadId": "2715e11441a6d424"},
        ]
        # run
        client = GmailClient("auth_token_path", "user_id", "version")
        actual = client._raw_search_gmail(
            search_query, label_ids, include_spam_trash, results_per_page, max_search_results
        )
        # assert
        assert expected == list(actual)

    def test_extract_href_from_html_positive_values(self):
        # setup
        html = """
        <a href='http://www.test1.com'>click me</a>
        <a href="http://www.test2.com">click me</a>
        <a  href = 'http://www.test3.com' > click me </a>
        <a  href = "http://www.test4.com" > click me </a>
        <a> click me </a>
        """
        expected = set(["http://www.test1.com", "http://www.test2.com", "http://www.test3.com", "http://www.test4.com"])
        # run
        actual = GmailClient._extract_href_from_html(html)
        # assert
        assert list(actual).sort() == list(expected).sort()

    def test_extract_href_from_html_no_values(self):
        # setup
        html = ""
        expected = set([])
        # run
        actual = GmailClient._extract_href_from_html(html)
        # assert
        assert list(actual).sort() == list(expected).sort()

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._extract_href_from_html")
    @patch("base64.urlsafe_b64decode")
    def test_convert_to_url_list_successful_pass(self, mock_b64decode, mock_extract_href):
        # setup
        message = self.sample_gmail_response
        message_id = message.get("id")
        html = "some html"
        expected = [Url(message_id, "https://www.download.com/file.csv")]
        # mock
        mock_b64decode.return_value = bytes(html, "UTF-8")
        mock_extract_href.side_effect = [set(["https://www.download.com/file.csv"])]
        # run
        actual = GmailClient._convert_to_url_list(message)
        # assert
        mock_b64decode.assert_called_once_with("base64encodedHtmlData")
        mock_extract_href.assert_called_once_with(html)
        assert expected == actual

    @patch("tap_gmail_csv.gmail_client.client.GmailClient._extract_href_from_html")
    @patch("base64.urlsafe_b64decode")
    def test_convert_to_url_list_with_no_matches_gives_empty_list(self, mock_b64decode, mock_extract_href):
        # setup
        message = self.sample_gmail_response
        message["payload"]["parts"] = []
        html = "some html"
        expected = []
        # mock
        mock_b64decode.return_value = bytes(html, "UTF-8")
        mock_extract_href.side_effect = [set(["https://www.download.com/file.csv"])]
        # run
        actual = GmailClient._convert_to_url_list(self.sample_gmail_response)
        # assert
        mock_b64decode.assert_not_called()
        mock_extract_href.assert_not_called()
        assert expected == actual


if __name__ == "__main__":
    unittest.main()
