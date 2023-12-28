from queue import Queue
import requests
from urllib.parse import urlparse
from datetime import datetime

class Scraper:
  def __init__(self, process_page):
    self.process_page = process_page

    self.urls = Queue()
    with open('urls', 'r') as file:
      # self.urls = [line.strip() for line in file]
      for line in file:
        self.urls.put(line.strip())

  def scrape(self):
    processed_urls_file = open('processed_urls.csv', 'w')

    while not self.urls.empty():
      url = self.urls.get()
      response = requests.get(url)

      if response.status_code == 200:
        parsed_url = urlparse(url)
        url_domain = parsed_url.netloc
        url_path = parsed_url.path

        self.process_page(response.text, url_domain, url_path)

        processed_urls_file.write(f"{url}, {self._formatted_current_time()}\n")

    processed_urls_file.close()

  def _formatted_current_time(self):
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

def process_page(input_html, domain, url_path):
  pass

scraper = Scraper(process_page)
scraper.scrape()
