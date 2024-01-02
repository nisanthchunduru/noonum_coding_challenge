from freezegun import freeze_time
import csv

from scraper import Scraper

@freeze_time("2000-01-01")
def test_scraping():
	def process_page(*args):
		pass

	urls = [
		"https://mock.codes/200",
	]
	scraper = Scraper(process_page)
	scraper.scrape(urls=urls, parallelism=1)

	with open("processed_urls.csv") as file:
		actual_processed_urls_csv = file.read()
		expected_urls_csv = """https://mock.codes/200; 2000-01-01T00:00:00
"""
		assert actual_processed_urls_csv == expected_urls_csv

@freeze_time("2000-01-01")
def test_page_processing_exception_handling():
	def process_page(*args):
		raise Exception("Oops, something's went wrong")

	urls = [
		"https://mock.codes/200",
		"https://mock.codes/200"
	]
	scraper = Scraper(process_page)
	scraper.scrape(urls=urls, parallelism=1)

	with open("bad_urls.csv") as file:
		actual_bad_urls_csv = file.read()
		expected_bad_urls_csv = """https://mock.codes/200; 2000-01-01T00:00:00; Oops, something's went wrong
https://mock.codes/200; 2000-01-01T00:00:00; Skipped (Oops, something's went wrong)
"""
		assert actual_bad_urls_csv == expected_bad_urls_csv

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
		expected_bad_urls_csv = """https://mock.codes/200; 2000-01-01T00:00:00; ROBOT_WARNING_DETECTED
https://mock.codes/200?hello=world; 2000-01-01T00:00:00; Skipped (ROBOT_WARNING_DETECTED)
"""
		assert actual_bad_urls_csv == expected_bad_urls_csv

@freeze_time("2000-01-01")
def test_non_existant_url_handling():
	def process_page(*args):
		pass

	urls = [
		"https://google2.com"
	]
	scraper = Scraper(process_page)
	scraper.scrape(urls=urls, parallelism=1)
	with open("bad_urls.csv") as file:
		rows = list(csv.reader(file, delimiter=';'))
		assert len(rows), 1
		row = rows[0]
		assert row[0], "https://google2.com"
		assert row[1], "2000-01-01T00:00:00"
		assert ("NameResolutionError" in row[2]), True
