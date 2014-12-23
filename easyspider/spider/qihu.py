import calendar
import time
import datetime
import logging

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import re

from easyspider.spider.spider import Extractor, Spider


class QihuLeftExtractor(Extractor):
    def extract(self, soup):
        return [link.string for link in soup.find('div', attrs={"id": "m-spread-left"}).find_all('cite')]


class QihuRightExtractor(Extractor):
    def extract(self, soup):
        return [link.string for link in soup.find('div', attrs={"id": "right_show"}).find('ul').find_all('cite')]


class QihuBrandExtractor(Extractor):
    def extract(self, soup):
        return [link['href'] for link in soup.find('div', attrs={"id": "sunrise"}).find_all('a')]


class QihuSpider(Spider):
    def __init__(self, options={}):
        super(QihuSpider, self).__init__(options=options)
        self.options = options
        self.source = "qihu"
        self.timestamp = calendar.timegm(datetime.datetime.utcnow().timetuple())
        self.extractors = {
            'leftlinks': QihuLeftExtractor(),
            'rightlinks': QihuRightExtractor(),
            'brandlinks': QihuBrandExtractor()
        }

        self.url = "http://www.so.com/"
        self.driver.get(self.url)

    def crawl(self, keyword):
        logging.info("processing: %s" % keyword)
        self.timestamp = calendar.timegm(datetime.datetime.utcnow().timetuple())  # refresh time stamp
        result = self.parse(self.download(keyword), keyword)
        result['keyword'] = keyword
        result['timestamp'] = self.timestamp
        result['source'] = self.source
        return result

    def download(self, keyword):
        start = time.time()
        self.driver.get(self.url)
        searchbox_id = 'input'
        searchbox = self.driver.find_element_by_id(searchbox_id)
        searchbox.clear()
        searchbox.send_keys(keyword)
        searchbox.send_keys(Keys.ENTER)
        # clear because we must use content left to tell if a new search is done
        self.driver.set_window_size(1980, 1280)
        try:
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "main"))
            )
        except TimeoutException as te:
            logging.error(te.message)
            # if timeout we just swallow at this moment, assuming mgr will find words are missing
        finally:
            if 'keyword_screenshot' in self.options \
                    and self.options['keyword_screenshot'] is True:
                self.driver.save_screenshot(
                    '/tmp/screenshot/%s-%s-%s.png' % ( self.source,
                                                       keyword, self.timestamp))
            end = time.time()
            delta = end - start
            logging.info(self.driver.current_url)
            logging.info("Page loaded in :%s seconds " % delta)

        return self.driver.page_source

