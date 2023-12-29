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
      exception_name = e.__class__.__name__
      reason = exception_name
      self._add_url_to_bad_urls_file(reason)
      next

    self._add_url_to_processed_urls_file(url)

  def _add_url_to_processed_urls_file(self, url):
    line = f"{url}, {formatted_current_time()}\n"
    self.processed_urls_file.write(line)
