from urllib.parse import urlparse
from datetime import datetime, timedelta
from collections import defaultdict

from scraper.plugins.bad_urls_csv_generator import BadUrlsCsvGenerator

class UnsuccessfulScrapeHandler:
  DOMAIN_502_LIMIT = 2
  RETRY_DELAY_AFTER_502 = 60

  def __init__(self, scraper):
    self.scraper = scraper
    self.domains_last_502_timestamps = {}
    self.domains_502_counts = defaultdict(int)

  def handle_url(self, url):
    parsed_url = urlparse(url)
    url_domain = parsed_url.netloc

    # Skip the url if the domain is offline
    if not url_domain in self.domains_last_502_timestamps:
      return True

    domain_502_count = self.domains_502_counts[url_domain]
    if domain_502_count >= self.DOMAIN_502_LIMIT:
      reason = "domain_502_limit_exceeded"
      self._dispatch_scrape_failed_event(url, reason)
      return False

    retry_at = self.domains_last_502_timestamps[url_domain] + timedelta(seconds=self.RETRY_DELAY_AFTER_502)
    if datetime.now() < retry_at:
      self.scraper.add_url(url, retry_at)
      return False

    return True

  def handle_response(self, url, response):
    if response.status_code == 200:
      return True

    if not response.status_code == 502:
      reason = response.status_code
      self._dispatch_scrape_failed_event(url, reason)
      return True

    parsed_url = urlparse(url)
    url_domain = parsed_url.netloc

    self.domains_502_counts[url_domain] = self.domains_502_counts[url_domain] + 1

    if self.domains_502_counts[url_domain] >= self.DOMAIN_502_LIMIT:
      reason = 502
      self._dispatch_url_skipped_event(url, reason)
    else:
      current_time = datetime.now()
      self.domains_last_502_timestamps[url_domain] = current_time
      retry_at = current_time + timedelta(seconds=self.RETRY_DELAY_AFTER_502)
      self.scraper.add_url(url, retry_at)

    return True

  def _dispatch_url_skipped_event(self, url, reason):
    context = {
      "url": url,
      "reason": reason
    }
    self.scraper.dispatch_event("url_skipped", context)

  def _dispatch_scrape_failed_event(self, url, reason):
    context = {
      "url": url,
      "reason": reason
    }
    self.scraper.dispatch_event("scrape_failed", context)
