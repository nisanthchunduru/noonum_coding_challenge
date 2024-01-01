from urllib.parse import urlparse

class PageProcessingExceptionHandler:
  def __init__(self, scraper):
    self.scraper = scraper
    self.affected_urls = {}

  def handle_url(self, url):
    if url in self.affected_urls:
      context = {
        "url": url,
        "reason": self.affected_urls[url]
      }
      self.scraper.dispatch_event("url_skipped", context)
      return False

    return True

  def handle_event(self, event, context):
    if event == "scrape_failed":
      reason = context["reason"]
      if isinstance(reason, Exception) and str(reason) != "ROBOT_WARNING_DETECTED":
        url = context["url"]
        self.affected_urls[url] = str(reason)
