from queue import PriorityQueue
import requests
from urllib.parse import urlparse
from datetime import datetime, timedelta
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from scraper.plugins.bad_urls_csv_generator import BadUrlsCsvGenerator
from scraper.plugins.successful_scrape_handler import SuccessfulScrapeHandler
from scraper.plugins.unsuccessful_scrape_handler import UnsuccessfulScrapeHandler

class Scraper:
  def __init__(self, process_page):
    self.process_page = process_page

  def scrape(self, parallelism=2):
    self._load_urls()
    self.plugins = [BadUrlsCsvGenerator(self), SuccessfulScrapeHandler(self), UnsuccessfulScrapeHandler(self)]
    executor = ThreadPoolExecutor(max_workers=parallelism)
    self.workers = {}

    while True:
      if len(self.workers) >= parallelism:
        # Wait for a worker to become done
        wait(self.workers.values(), None, return_when=FIRST_COMPLETED)
        next

      if self.urls.empty():
        if len(self.workers) == 0:
          break
        else:
          # Wait for atleast on worker to be done
          wait(self.workers.values(), None, return_when=FIRST_COMPLETED)
          continue
      else:
          if len(self.workers) == parallelism:
            # Wait for atleast one worker to be done
            wait(self.workers.values(), None, return_when=FIRST_COMPLETED)

      priority, url = self.urls.get()
      scrape_at = datetime.fromtimestamp(priority)
      current_time = datetime.now()
      if scrape_at > current_time:
        self._sleep_until(scrape_at)

      skip_url = False
      for plugin in self.plugins:
        if hasattr(plugin, 'handle_url'):
            skip_url = not plugin.handle_url(url)

      if skip_url:
        continue

      worker = executor.submit(self._get_url, url)
      worker.add_done_callback(self._handle_response)
      self.workers[url] = worker

  def add_url(self, url, scrape_at=None):
    if scrape_at == None:
      priority = 0
    else:
      priority = int(scrape_at.timestamp())
    self.urls.put((priority, url))

  def scrape_url_at(self, url, scrape_at):
    self.add_url(url, scrape_at=scrape_at)

  def dispatch_command(self, command, *args):
    for plugin in self.plugins:
      if hasattr(plugin, 'handle_command'):
        plugin.handle_command(command, *args)

  def _load_urls(self):
    self.urls = PriorityQueue()
    with open('urls', 'r') as file:
      for line in file:
        url = line.strip()
        self.add_url(url)

  def _get_url(self, url):
    response = requests.get(url)
    return (url, response)

  def _handle_response(self, worker):
    url, response = worker.result()

    del self.workers[url]

    for plugin in self.plugins:
      if hasattr(plugin, 'handle_response'):
        plugin.handle_response(url, response)

  def _sleep_until(self, target_time):
    current_time = datetime.now()
    if current_time > target_time:
      return

    delta = (target_time - current_time).total_seconds()
    time.sleep(delta)

def process_page(input_html, domain, url_path):
  pass

scraper = Scraper(process_page)
scraper.scrape()
