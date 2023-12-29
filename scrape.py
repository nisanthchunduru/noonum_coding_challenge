from queue import PriorityQueue
import requests
from urllib.parse import urlparse
from datetime import datetime, timedelta
import time
from collections import defaultdict

class Scraper:
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
    max_acceptable_domain_502s = 2
    wait_period_after_502 = 60
    # wait_period_after_502 = 5

    while not self.urls.empty():
      priority, url = self.urls.get()
      scrape_at = datetime.fromtimestamp(priority)
      current_time = datetime.now()
      if scrape_at > current_time:
        self._sleep_until(scrape_at)

      parsed_url = urlparse(url)
      url_domain = parsed_url.netloc
      url_path = parsed_url.path

      if url_domain in self.five02_domains:
        unreachable_until = self.five02_domains[url_domain]
        if self.domain_502_counts[url_domain] >= max_acceptable_domain_502s:
          reason = "max_acceptable_domain_502s_reached"
          self._add_url_to_processed_urls_file(url, reason)
          next
        elif datetime.now() < unreachable_until:
          self._scrape_url_at(url, unreachable_until)
          next

      response = requests.get(url)

      if response.status_code == 200:
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

        if self.domain_502_counts[url_domain] >= max_acceptable_domain_502s:
          reason = 502
          self._add_url_to_bad_urls_file(url, reason)
        else:
          self._scrape_url_in(url, wait_period_after_502)
      else:
        reason = response.status_code
        self._add_url_to_bad_urls_file(url, reason)

    self.processed_urls_file.close()
    self.bad_urls_file.close()

  def _formatted_current_time(self):
    return datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

  def _sleep_until(self, target_time):
    current_time = datetime.now()
    if current_time > target_time:
      return

    delta = (target_time - current_time).total_seconds()
    time.sleep(delta)

  def _scrape_url_at(self, url, scrape_at):
    priority = int(scrape_at.timestamp())
    self.urls.put((priority, url))

  def _scrape_url_in(self, url, seconds):
    current_time = datetime.now()
    self._scrape_url_at(url, current_time + timedelta(seconds=seconds))

  def _add_url_to_processed_urls_file(self, url):
      line = f"{url}, {self._formatted_current_time()}\n"
      self.processed_urls_file.write(line)

  def _add_url_to_bad_urls_file(self, url, reason):
    line = f"{url}, {self._formatted_current_time()}, {reason}\n"
    self.bad_urls_file.write(line)

def process_page(input_html, domain, url_path):
  pass

scraper = Scraper(process_page)
scraper.scrape()
