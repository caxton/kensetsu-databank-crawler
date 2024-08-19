import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import random
import sys
from concurrent.futures import ThreadPoolExecutor

# Constants
BASE_URL = "https://www.kensetsu-databank.co.jp/osirase/detail.php?id="
EXCEL_FILE = "kensetsu_data.xlsx"
TABLE_CLASS = 'san-two'
LINK_COLUMN = "link"

def crawl_and_extract_data(url):
    """
    Crawls a webpage and extracts the title and table data.

    Args:
        url: The URL of the webpage to crawl.

    Returns:
        A dictionary containing the extracted data, or None if an error occurs.
    """
    try:
        # print(f"Crawling: {url}")
        response = requests.get(url)
        response.raise_for_status() # returns an HTTPError object if an error has occurred

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract table data (using th and td tags for key-value pairs)
        table = soup.find('table', class_=TABLE_CLASS)
        data = {}

        if table:
            rows = table.find_all('tr')
            for row in rows:
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    key = th.text.strip()
                    value = td.text.strip()
                    data[key] = value

        # Append the link to the end of data
        if data:
            data[LINK_COLUMN] = url

        return {**data}

    except requests.exceptions.RequestException as e:
        print(f"Error crawling {url}: {e}")
        return None

def crawl_kensetsu_databank_range(start_id, end_id, append_to_existing=True, num_threads=1):
    """
    Crawls kensetsu-databank detail pages within a specified ID range using multithreading.

    Args:
        start_id: The starting ID of the range.
        end_id: The ending ID of the range.
        append_to_existing: If True (default), appends new data to the existing Excel file. 
                            If False, overwrites the file.
        num_threads: The number of threads to use for crawling (default 1).
    """
    base_url = BASE_URL
    all_data = []
    error_links = []
    batch_size = 32

    total_ids = end_id - start_id + 1

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit crawling tasks to the thread pool
        futures = [executor.submit(crawl_and_extract_data, base_url + str(id)) for id in range(start_id, end_id + 1)]

        # Process results as they become available
        for i, future in enumerate(futures):
            try:
                extracted_data = future.result()
                if extracted_data:
                    all_data.append(extracted_data)
                else:
                    error_links.append(base_url + str(start_id + i))

                # Append to Excel in batches
                if len(all_data) >= batch_size:
                    append_or_create_excel(all_data, append_to_existing)
                    all_data = []

                # Display progress on the same line
                progress = round((i+1) / total_ids, 3) * 100
                print(f"\rCrawled {i+1}/{total_ids} ({progress:.1f}%): {base_url + str(start_id + i)}, cache size: {len(all_data)}", end="")

                # Introduce a delay between requests
                # time.sleep(0.2)

                # Randomly sleep for a longer duration to avoid heavy traffic
                # if random.random() < 0.01:
                #     sleep_duration = random.randint(3, 7)
                #     print(f"\rSleeping for {sleep_duration} seconds to avoid heavy traffic...", end="")
                #     time.sleep(sleep_duration)

            except Exception as e:
                print(f"Error in thread: {e}")

    # Append any remaining data
    if all_data:
        append_or_create_excel(all_data, append_to_existing)

    print("\n")

def append_or_create_excel(data, append_to_existing):
    """
    Appends data to an existing Excel file or creates a new one.

    Args:
        data: The list of dictionaries containing the extracted data.
        append_to_existing: If True, appends to the existing file; otherwise, creates a new one.
    """
    if append_to_existing and os.path.exists(EXCEL_FILE):
        existing_df = pd.read_excel(EXCEL_FILE)
        new_df = pd.DataFrame(data)

        combined_df = pd.concat([existing_df, new_df])
        combined_df.drop_duplicates(subset=[LINK_COLUMN], keep="last", inplace=True)

        combined_df.to_excel(EXCEL_FILE, index=False)
        print(f"\nData appended to {EXCEL_FILE}")
    else:
        df = pd.DataFrame(data)
        df.to_excel(EXCEL_FILE, index=False)
        print(f"\nData exported to {EXCEL_FILE}")

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python script_name.py start_id end_id [num_threads]")
        sys.exit(1)

    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])
    num_threads = int(sys.argv[3]) if len(sys.argv) == 4 else 1  # Default to 1 threads

    crawl_kensetsu_databank_range(start_id, end_id, num_threads=num_threads)
