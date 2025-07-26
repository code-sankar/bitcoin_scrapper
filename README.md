Scrapping bitcoin Database : Wallet address -> Wallet private key

1. Packages used->   pip install requests beautifulsoup4 pandas fake-useragent tqdm urllib3

2. Optional (if using proxies or advanced features):
   (if using SOCKS proxies):  pip install socks
   (for SSL verification bypass) : pip install pyopenssl

3. How to run:
  - Start with test mode: python bitcoin_scraper.py --scrape --test
  - For full scrape: python bitcoin_scraper.py --scrape --max-pages 50
  - Use proxy rotation: --proxy auto (default)
  - Search database: python bitcoin_scraper.py --search 1A1zP --format console
