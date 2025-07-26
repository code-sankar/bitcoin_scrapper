import os
import time
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from fake_useragent import UserAgent
import argparse
import logging
from tqdm import tqdm
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Suppress warnings
import warnings
warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bitcoin_scraper.log'),
        logging.StreamHandler()
    ]
)

BASE_URL = "https://privatekeyfinder.io/private-keys/bitcoin/"
USER_AGENTS = UserAgent()
REQUEST_TIMEOUT = 30
MIN_DELAY = 3.0  
MAX_DELAY = 7.0   
RETRY_LIMIT = 5   
SAVE_INTERVAL = 3  
PROXY_LIST = [
    '',
    '',
    '',
    '',
    '', # Add your proxy links here 
]

class BitcoinPrivateKeyScraper:

    def __init__(self, output_file='bitcoin_keys.csv', max_pages=None, test_mode=False, proxy=None):
        self.output_file = output_file
        self.max_pages = max_pages
        self.test_mode = test_mode
        self.scraped_data = []
        self.proxy = proxy
        self.session = self._create_session()
        self.proxy_index = 0
        self.proxies = PROXY_LIST

    def _create_session(self):
        session = requests.Session()
        
        # More robust retry strategy
        retry_strategy = Retry(
            total=RETRY_LIMIT,
            status_forcelist=[429, 403, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1.5,
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=50, pool_maxsize=50)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        session.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'DNT': '1',
            'Referer': BASE_URL
        })
        return session

    def _get_random_delay(self):
        return random.uniform(MIN_DELAY, MAX_DELAY)

    def _get_user_agent(self):
        return USER_AGENTS.random

    def _rotate_proxy(self):
        if not self.proxies:
            return None
            
        self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
        return self.proxies[self.proxy_index]

    def _request_with_retry(self, url):
        for attempt in range(RETRY_LIMIT):
            try:
                self.session.headers['User-Agent'] = self._get_user_agent()
                
                proxies = {}
                if self.proxy == 'auto':
                    proxy = self._rotate_proxy()
                    if proxy:
                        proxies = {'http': proxy, 'https': proxy}
                        logging.info(f"Using proxy: {proxy}")
                elif self.proxy and self.proxy != 'none':
                    proxies = {'http': self.proxy, 'https': self.proxy}
                
                response = self.session.get(
                    url, 
                    timeout=REQUEST_TIMEOUT,
                    proxies=proxies,
                    verify=False  
                )
                response.raise_for_status()

                blocking_indicators = [
                    'cloudflare', 'access denied', 'captcha', 
                    '403 forbidden', 'blocked', 'security check'
                ]
                if any(indicator in response.text.lower() for indicator in blocking_indicators):
                    raise requests.exceptions.RequestException("Blocking detected")

                if "table table-striped" not in response.text:
                    raise ValueError("Table content missing")

                return response
            except (requests.exceptions.RequestException, 
                    requests.exceptions.Timeout,
                    requests.exceptions.ProxyError,
                    ValueError) as e:
                wait = (2 ** attempt) + random.random()
                logging.warning(f"Attempt {attempt+1}/{RETRY_LIMIT} failed: {str(e)}. Retrying in {wait:.1f}s")
                time.sleep(wait)
        return None

    def _parse_page(self, html_content):
        """Parse HTML content and extract key data"""
        soup = BeautifulSoup(html_content, 'html.parser')
        table = soup.find('table', {'class': 'table-striped'})  

        if not table:
            logging.error("Table not found. Website structure may have changed.")
            return []

        rows = table.find_all('tr')[1:]  
        if not rows:
            logging.info("No rows found on page")
            return []

        page_data = []
        for row in rows:
            cols = row.find_all('td')
            if len(cols) < 5:
                continue

            details_link = cols[4].find('a')
            details_url = urljoin(BASE_URL, details_link['href']) if details_link else ""

            address = cols[1].text.strip()
            
            if not re.match(r'^(bc1|[13])[a-zA-HJ-NP-Z0-9]{25,90}$', address):
                logging.warning(f"Invalid Bitcoin address format: {address}")
                continue

            record = {
                'index': cols[0].text.strip(),
                'address': address,
                'balance': cols[2].text.strip(),
                'private_key': cols[3].text.strip(),
                'details_url': details_url,
                'timestamp': pd.Timestamp.now().isoformat()
            }
            page_data.append(record)

        return page_data

    def _save_progress(self, force_save=False):
        if not self.scraped_data:
            return

        try:
            df = pd.DataFrame(self.scraped_data)

            if os.path.exists(self.output_file):
                existing_df = pd.read_csv(self.output_file)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_csv(self.output_file, index=False)
            else:
                df.to_csv(self.output_file, index=False)

            if force_save:
                logging.info(f"Saved {len(df)} records to {self.output_file}")
        except Exception as e:
            logging.error(f"Failed to save progress: {str(e)}")
        finally:
            self.scraped_data = []

    def scrape_database(self):
        logging.info("Starting Bitcoin private key scraping")

        page = 1
        total_records = 0
        consecutive_empty = 0

        try:
            with tqdm(desc="Scraping Pages", unit="page") as pbar:
                while True:
                    if self.max_pages and page > self.max_pages:
                        logging.info(f"Reached max pages limit ({self.max_pages})")
                        break
                    if self.test_mode and page > 2:
                        logging.info("Test mode complete")
                        break

                    url = f"{BASE_URL}?page={page}" if page > 1 else BASE_URL

                    delay = self._get_random_delay()
                    time.sleep(delay)

                    response = self._request_with_retry(url)
                    if not response:
                        logging.error(f"Failed to retrieve page {page}")
                        break

                    page_data = self._parse_page(response.text)
                    if not page_data:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            logging.info("3 consecutive empty pages. Stopping.")
                            break
                    else:
                        consecutive_empty = 0
                        self.scraped_data.extend(page_data)
                        total_records += len(page_data)

                    if page % SAVE_INTERVAL == 0:
                        self._save_progress()

                    pbar.update(1)
                    pbar.set_postfix(records=total_records, page=page)

                    # Check for last page
                    soup = BeautifulSoup(response.text, 'html.parser')
                    next_button = soup.find('a', {'rel': 'next'})
                    if not next_button:
                        logging.info("No more pages found")
                        break

                    page += 1

        except KeyboardInterrupt:
            logging.info("Scraping interrupted by user")
        except Exception as e:
            logging.exception(f"Critical error: {str(e)}")
        finally:
            self._save_progress(force_save=True)
            logging.info(f"Scraping complete. Total records: {total_records}")
            self.session.close()

        return total_records

    def search_database(self, keyword, output_format='console'):
        """Search the database for a keyword"""
        logging.info(f"Searching for keyword: '{keyword}'")

        if not os.path.exists(self.output_file):
            logging.error("Database file not found. Please scrape first.")
            return []

        try:
            df = pd.read_csv(self.output_file)

            mask = df.apply(lambda row: row.astype(str).str.contains(keyword, case=False).any(), axis=1)
            results = df[mask]

            if results.empty:
                logging.info("No matching records found")
                return []

            if output_format == 'console':
                print("\nSearch Results:")
                print(results[['index', 'address', 'balance', 'private_key', 'details_url']].to_string(index=False))
            elif output_format == 'csv':
                output_file = f"search_results_{keyword}.csv"
                results.to_csv(output_file, index=False)
                logging.info(f"Results saved to {output_file}")
            elif output_format == 'json':
                output_file = f"search_results_{keyword}.json"
                results.to_json(output_file, orient='records')
                logging.info(f"Results saved to {output_file}")

            logging.info(f"Found {len(results)} matching records")
            return results.to_dict('records')

        except Exception as e:
            logging.error(f"Search failed: {str(e)}")
            return []

def main():
    parser = argparse.ArgumentParser(
        description='Bitcoin Private Key Database System',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--scrape', action='store_true', help='Scrape keys from website')
    parser.add_argument('--max-pages', type=int, default=None, help='Maximum pages to scrape')
    parser.add_argument('--test', action='store_true', help='Test mode (limit to 2 pages)')
    parser.add_argument('--search', type=str, help='Keyword to search in database')
    parser.add_argument('--output', type=str, default='bitcoin_keys.csv', help='Output CSV filename')
    parser.add_argument('--format', choices=['console', 'csv', 'json'], default='console',
                        help='Output format for search results')
    parser.add_argument('--proxy', type=str, default='auto', 
                        help='Proxy URL () or "auto" for rotation') # Put your proxy link here #

    args = parser.parse_args()

    scraper = BitcoinPrivateKeyScraper(
        output_file=args.output,
        max_pages=args.max_pages,
        test_mode=args.test,
        proxy=args.proxy
    )

    if args.scrape:
        scraper.scrape_database()
    elif args.search:
        scraper.search_database(args.search, args.format)
    else:
        parser.print_help()
        print("\nExamples:")
        print("  python bitcoin_scraper.py --scrape --max-pages 10")
        print("  python bitcoin_scraper.py --search 1A1zP --format console")

if __name__ == "__main__":
    print("""
 Bitcoin Wallet address to Wallet Private Key               
""")
    main()