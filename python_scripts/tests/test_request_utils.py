import os
import sys

# This is needed to run debug with VSCode
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import json
import unittest
from unittest.mock import Mock, patch

import requests
from shared import request_utils
from shared.request_utils import get_api_token


class TestRequestUtils(unittest.TestCase):
    @patch("requests.post")
    def test_retry_post_request_success(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        url = "http://test.com"
        data = {"key": "value"}

        # Act
        response = request_utils.retry_post_request(url, data=json.dumps(data))

        # Assert
        mock_post.assert_called_once_with(url, data=json.dumps(data))
        self.assertEqual(response, mock_response)

    @patch("requests.post")
    def test_retry_post_request_retry(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 500
        mock_post.return_value = mock_response
        url = "http://test.com"
        data = {"key": "value"}

        # Act
        with self.assertRaises(requests.exceptions.ConnectionError):
            request_utils.retry_post_request(url, data=json.dumps(data))

        # Assert
        self.assertEqual(mock_post.call_count, 20)

    @patch("requests.post")
    def test_retry_post_request_connection_error(self, mock_post):
        # Arrange
        mock_post.side_effect = requests.exceptions.ConnectionError
        url = "http://test.com"
        data = {"key": "value"}

        # Act
        with self.assertRaises(requests.exceptions.ConnectionError):
            request_utils.retry_post_request(url, data=json.dumps(data))

        # Assert
        self.assertEqual(mock_post.call_count, 20)

    @patch("requests.get")
    def test_retry_get_request_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        url = "http://test.com"
        query_string = {"key": "value"}

        # Act
        response = request_utils.retry_get_request(url, query_string=query_string)

        # Assert
        mock_get.assert_called_once_with(url, params=query_string)
        self.assertEqual(response, mock_response)

    @patch("requests.get")
    def test_retry_get_request_retry(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response
        url = "http://test.com"
        query_string = {"key": "value"}

        with self.assertRaises(requests.exceptions.ConnectionError):
            request_utils.retry_get_request(url, query_string=query_string)

        self.assertEqual(mock_get.call_count, 20)

    @patch("requests.get")
    def test_retry_get_request_connection_error(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError
        url = "http://test.com"
        query_string = {"key": "value"}

        with self.assertRaises(requests.exceptions.ConnectionError):
            request_utils.retry_get_request(url, query_string=query_string)

        self.assertEqual(mock_get.call_count, 20)

    @patch("shared.request_utils.retry_post_request")
    def test_get_api_token_success(self, mock_post):
        mock_response = Mock()
        mock_response.headers = {"xsrf-token": "test_token"}
        mock_post.return_value = mock_response

        token = get_api_token()

        self.assertEqual(token, "test_token")

    @patch("shared.request_utils.retry_post_request")
    def test_get_api_token_failure(self, mock_post):
        mock_post.return_value = None

        with self.assertRaises(Exception, msg="Failed to get FusionSolar API token"):
            get_api_token()

    @patch("shared.request_utils.retry_post_request")
    def test_get_api_token_missing_header(self, mock_post):
        mock_response = Mock()
        mock_response.headers = {}
        mock_post.return_value = mock_response

        with self.assertRaises(KeyError):
            get_api_token()


if __name__ == "__main__":
    unittest.main()
