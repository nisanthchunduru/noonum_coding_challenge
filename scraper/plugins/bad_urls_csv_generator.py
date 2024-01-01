from scraper.utils import formatted_current_time

class BadUrlsCsvGenerator:
  def __init__(self, scraper):
    self.scraper = scraper
    self.bad_urls_csv_file = open('bad_urls.csv', 'w')

  def handle_event(self, event, context):
    if event == "scrape_failed":
      url = context["url"]
      reason = context["reason"]
      if isinstance(reason, Exception):
        e = reason
        reason = str(e)
      line = f"{url}, {formatted_current_time()}, {reason}\n"
      self.bad_urls_csv_file.write(line)
      self.bad_urls_csv_file.flush()
    elif event == "url_skipped":
      url = context["url"]
      reason = context["reason"]
      line = f"{url}, {formatted_current_time()}, Skipped ({reason})\n"
      self.bad_urls_csv_file.write(line)
      self.bad_urls_csv_file.flush()
