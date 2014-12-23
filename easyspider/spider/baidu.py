import calendar
import datetime
import json
import time
import logging
import os

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import re

from easyspider.spider.spider import Extractor, Spider

"""
  Baidu related spider, content extractor
"""


class BaiduLeftExtractor(Extractor):  # shown on the left
    def extract(self, soup):
        return [link.previous_sibling.find('span').string for link in
                soup.find_all('div',
                              attrs={
                                  'id': re.compile('tools_(400.|500.)')})]


class BaiduBrandAdvExtractor(Extractor):
    def extract(self, soup):
        result = []
        brand_advs = soup.find_all('div', attrs={'class': 'ad-block-4'})  # upper right advertiser
        for badv in brand_advs:
            badv_name = badv.find('div', attrs={'class': 'ec-site'}).find('a').string
            result.append(badv_name)
        return result


class BaiduRightExtractor(Extractor):
    def extract(self, soup):
        result = []
        right_links = [link.previous_sibling for link in
                       soup.find('div', attrs={'id': 'ec_im_container'}).find_all('div',
                                                                                  attrs={
                                                                                      'id': re.compile('tools_.')})]
        for rlink in right_links:
            fonts = rlink.find_all('font')
            result.append(fonts[len(fonts) - 1].string)
        return result


class BaiduSpider(Spider):
    def __init__(self, options={}):
        super(BaiduSpider, self).__init__(options=options)
        self.options = options
        self.source = "baidu"
        self.timestamp = calendar.timegm(datetime.datetime.utcnow().timetuple())
        self.extractors = {
            'leftlinks': BaiduLeftExtractor(),
            'rightlinks': BaiduRightExtractor(),
            'brandlinks': BaiduBrandAdvExtractor()
        }

        self.url = "http://www.baidu.com/"
        cookie_path = os.path.join(options['application_root'], 'cookies', 'baidu.cookie')
        cookies = json.load(open(cookie_path))
        self.driver.get(self.url)
        for c in cookies:
            self.driver.add_cookie(c)

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
        self.driver.find_element_by_id('kw').click()
        self.driver.find_element_by_id('kw').clear()
        # clear because we must use content left to tell if a new search is done
        self.driver.find_element_by_id('kw').send_keys(keyword)
        self.driver.find_element_by_id('kw').send_keys(Keys.SPACE)
        self.driver.find_element_by_id('kw').send_keys(Keys.BACK_SPACE)
        self.driver.find_element_by_id('kw').send_keys(Keys.ENTER)
        self.driver.set_window_size(1980, 1280)
        try:
            element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "content_left"))
            )
        except TimeoutException as te:
            logging.error(te.message)
            # if timeout we just swallow at this moment, assuming mgr will find words are missing
        finally:
            if 'keyword_screenshot' in self.options \
                    and self.options['keyword_screenshot'] is True:
                self.driver.save_screenshot(
                    '/tmp/screenshot/screenshot-%s-%s.png' % (
                        keyword, self.timestamp))
            logging.info(self.driver.current_url)
            end = time.time()
            delta = end - start
            logging.info("Page loaded in :%s seconds " % delta)
        return self.driver.page_source

