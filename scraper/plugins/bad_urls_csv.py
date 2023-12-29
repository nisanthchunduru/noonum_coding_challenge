from scraper.utils import formatted_current_time

class BadUrlsCsv:
  def __init__(self, scraper):
    self.scraper = scraper
    self.bad_urls_file = open('bad_urls.csv', 'w')

  def add_url(self, url, reason):
    line = f"{url}, {formatted_current_time()}, {reason}\n"
    self.bad_urls_file.write(line)
    self.bad_urls_file.flush()
