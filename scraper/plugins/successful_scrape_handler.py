from urllib.parse import urlparse

from scraper.utils import formatted_current_time

class SuccessfulScrapeHandler:
  def __init__(self, scraper):
    self.scraper = scraper
    self.processed_urls_file = open('processed_urls.csv', 'w')

  def handle_response(self, url, response):
    if not response.status_code == 200:
      return True

    parsed_url = urlparse(url)
    url_domain = parsed_url.netloc
    url_path = parsed_url.path

    try:
      # TODO: Process page in a thread
      self.scraper.process_page(response.text, url_domain, url_path)
    except Exception as e:
      reason = e
      self._dispatch_scrape_failed_event(url, reason)
      return

    self._add_url_to_processed_urls_file(url)

  def _dispatch_scrape_failed_event(self, url, reason):
    context = {
      "url": url,
      "reason": reason
    }
    self.scraper.dispatch_event("scrape_failed", context)

  def _add_url_to_processed_urls_file(self, url):
    line = f"{url}; {formatted_current_time()}\n"
    self.processed_urls_file.write(line)
    self.processed_urls_file.flush()
