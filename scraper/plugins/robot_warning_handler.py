from urllib.parse import urlparse

class RobotWarningHandler:
  def __init__(self, scraper):
    self.scraper = scraper
    self.affected_domains = {}

  def handle_url(self, url):
    parsed_url = urlparse(url)
    url_domain = parsed_url.netloc

    if url_domain in self.affected_domains:
      context = {
        "url": url,
        "reason": self.affected_domains[url_domain]
      }
      self.scraper.dispatch_event("url_skipped", context)
      return False

    return True

  def handle_event(self, event, context):
    if event == "scrape_failed":
      reason = context["reason"]
      if isinstance(reason, Exception) and str(reason) == "ROBOT_WARNING_DETECTED":
        url = context["url"]
        parsed_url = urlparse(url)
        url_domain = parsed_url.netloc
        self.affected_domains[url_domain] = "ROBOT_WARNING_DETECTED"
