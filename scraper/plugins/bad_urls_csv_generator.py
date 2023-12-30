from scraper.utils import formatted_current_time

class BadUrlsCsvGenerator:
  def __init__(self, scraper):
    self.scraper = scraper
    self.bad_urls_file = open('bad_urls.csv', 'w')

  def handle_command(self, command, *args):
    if command == "add_url_to_bad_urls_csv":
      url = args[0]
      reason = args[1]
      self.add_url(url, reason)

  def add_url(self, url, reason):
    line = f"{url}, {formatted_current_time()}, {reason}\n"
    self.bad_urls_file.write(line)
    self.bad_urls_file.flush()
