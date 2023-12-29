from queue import PriorityQueue
import requests
from urllib.parse import urlparse
from datetime import datetime, timedelta
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED

class Scraper:
  PARALLELISM = 2
  DOMAIN_MAX_ACCEPTABLE_502_COUNT = 2
  RETRY_DELAY_AFTER_502 = 60

  def __init__(self, process_page):
    self.process_page = process_page

    self.urls = PriorityQueue()
    with open('urls', 'r') as file:
      # self.urls = [line.strip() for line in file]
      for line in file:
        self.urls.put((0, line.strip()))

    self.five02_domains = {}
    self.domain_502_counts = defaultdict(int)

  def scrape(self):
    self.processed_urls_file = open('processed_urls.csv', 'w')
    self.bad_urls_file = open('bad_urls.csv', 'w')
    executor = ThreadPoolExecutor(max_workers=self.PARALLELISM)
    self.workers = {}

    while True:
      if len(self.workers) >= self.PARALLELISM:
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
          if len(self.workers) == self.PARALLELISM:
            # Wait for atleast one worker to be done
            wait(self.workers.values(), None, return_when=FIRST_COMPLETED)

      priority, url = self.urls.get()
      scrape_at = datetime.fromtimestamp(priority)
      current_time = datetime.now()
      if scrape_at > current_time:
        self._sleep_until(scrape_at)

      parsed_url = urlparse(url)
      url_domain = parsed_url.netloc

      if url_domain in self.five02_domains:
        retry_at = self.five02_domains[url_domain]
        if self.domain_502_counts[url_domain] >= self.DOMAIN_MAX_ACCEPTABLE_502_COUNT:
          reason = "domain_max_acceptable_502_count_reached"
          self._add_url_to_bad_urls_file(url, reason)
          continue
        elif datetime.now() < retry_at:
          self._scrape_url_at(url, retry_at)
          continue

      worker = executor.submit(self._get_url, url)
      worker.add_done_callback(self._after_url_get)
      self.workers[url] = worker

    self.processed_urls_file.close()
    self.bad_urls_file.close()

  def _get_url(self, url):
    response = requests.get(url)
    return (url, response)

  def _after_url_get(self, worker):
    url, response = worker.result()

    del self.workers[url]

    parsed_url = urlparse(url)
    url_domain = parsed_url.netloc
    url_path = parsed_url.path

    if response.status_code == 200:
      if url_domain in self.five02_domains:
        del self.five02_domains[url_domain]

      try:
        self.process_page(response.text, url_domain, url_path)
      except Exception as e:
        exception_name = e.__class__.__name__
        reason = exception_name
        self._add_url_to_bad_urls_file(reason)
        next

      self._add_url_to_processed_urls_file(url)
    elif response.status_code == 502:
      self.domain_502_counts[url_domain] = self.domain_502_counts[url_domain] + 1

      if self.domain_502_counts[url_domain] >= self.DOMAIN_MAX_ACCEPTABLE_502_COUNT:
        reason = 502
        self._add_url_to_bad_urls_file(url, reason)
      else:
        current_time = datetime.now()
        retry_at = current_time + timedelta(seconds=self.RETRY_DELAY_AFTER_502)
        self.five02_domains[url_domain] = retry_at
        self._scrape_url_at(url, retry_at)
    else:
      reason = response.status_code
      self._add_url_to_bad_urls_file(url, reason)

  def _scrape_url_at(self, url, scrape_at):
    priority = int(scrape_at.timestamp())
    self.urls.put((priority, url))

  def _sleep_until(self, target_time):
    current_time = datetime.now()
    if current_time > target_time:
      return

    delta = (target_time - current_time).total_seconds()
    time.sleep(delta)

  def _add_url_to_processed_urls_file(self, url):
      line = f"{url}, {self._formatted_current_time()}\n"
      self.processed_urls_file.write(line)

  def _add_url_to_bad_urls_file(self, url, reason):
    line = f"{url}, {self._formatted_current_time()}, {reason}\n"
    self.bad_urls_file.write(line)

  def _formatted_current_time(self):
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

def process_page(input_html, domain, url_path):
  pass

scraper = Scraper(process_page)
scraper.scrape()
