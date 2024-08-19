import requests
import unittest
import pandas as pd
import os
from unittest import mock
from unittest.mock import patch

# Import your crawler module (assuming it's in the same directory)
import crawler_by_id

class TestCrawler(unittest.TestCase):

    @patch('requests.get')  # Mock the requests.get function
    def test_crawl_and_extract_data(self, mock_get):
        # Mock a successful response with sample HTML
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = """
        <div class="top-h1-style"><h1>Sample Title</h1></div>
        <table class="san-two">
            <tr><th>Key 1</th><td>Value 1</td></tr>
            <tr><th>Key 2</th><td>Value 2</td></tr>
        </table>
        """

        # Call the function with a sample URL
        url = "https://www.example.com/sample_page"
        result = crawler_by_id.crawl_and_extract_data(url)

        # Assert the expected output
        expected_data = {
            'Key 1': 'Value 1',
            'Key 2': 'Value 2',
            'link': url
        }
        self.assertEqual(result, expected_data)

    @patch('requests.get')
    def test_crawl_and_extract_data_error(self, mock_get):
        # Mock a failed response
        mock_get.side_effect = requests.exceptions.RequestException("Network error")

        # Call the function with a sample URL
        url = "https://www.example.com/error_page"
        result = crawler_by_id.crawl_and_extract_data(url)

        # Assert that None is returned in case of an error
        self.assertIsNone(result)

    @patch('crawler_by_id.crawl_and_extract_data')  # Mock the crawl_and_extract_data function
    def test_crawl_kensetsu_databank_range(self, mock_crawl_and_extract_data):
        # Mock the extracted data for a few IDs
        mock_crawl_and_extract_data.side_effect = [
            {'物件番号': 12345, 'link': 'https://.../12345'},
            {'物件番号': 67890, 'link': 'https://.../67890'},
            None,  # Simulate an error for one ID
        ]

        # Call the function with a sample range and append_to_existing=False (overwrite)
        crawler_by_id.crawl_kensetsu_databank_range(12345, 12347, append_to_existing=False)

        # Assert that the Excel file was created
        self.assertTrue(os.path.exists(crawler_by_id.EXCEL_FILE))

        # Read the Excel file and assert its contents
        df = pd.read_excel(crawler_by_id.EXCEL_FILE)
        expected_data = {
            '物件番号': [12345, 67890],
            'link': ['https://.../12345', 'https://.../67890'],
        }
        pd.testing.assert_frame_equal(df, pd.DataFrame(expected_data))

        # Clean up: delete the created Excel file
        os.remove(crawler_by_id.EXCEL_FILE)

if __name__ == '__main__':
    unittest.main()
