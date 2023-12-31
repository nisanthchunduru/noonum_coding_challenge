from scraper import Scraper
from freezegun import freeze_time

@freeze_time("2000-01-01")
def test_robot_warning_handling():
	def process_page(*args):
		raise Exception("ROBOT_WARNING_DETECTED")

	urls = [
		"https://mock.codes/200",
		"https://mock.codes/200?hello=world"
	]
	scraper = Scraper(process_page)
	scraper.scrape(urls=urls, parallelism=1)

	with open("bad_urls.csv") as file:
		actual_bad_urls_csv = file.read()
		expected_bad_urls_csv = """https://mock.codes/200, 2000-01-01T00:00:00, ROBOT_WARNING_DETECTED
https://mock.codes/200?hello=world, 2000-01-01T00:00:00, skipped - robot_warning_detected
"""
		assert actual_bad_urls_csv == expected_bad_urls_csv

test_robot_warning_handling()
