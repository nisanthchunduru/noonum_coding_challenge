import os

from scraper import Scraper

def process_page(input_html, domain, url_path):
  pass

scrape_options = {}
if 'PARALLELISM' in os.environ:
  scrape_options["parallelism"] = int(os.environ['PARALLELISM'])
scraper = Scraper(process_page)
scraper.scrape(**scrape_options)
