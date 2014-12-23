import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import tldextract


class Spider(object):
    def __init__(self, options={}):
        logging.info('initializing html render...')
        if (options['webdriver'] == 'chrome'):
            self.driver = webdriver.Chrome()
        else:
            dcap = dict(DesiredCapabilities.PHANTOMJS)
            dcap["phantomjs.page.settings.userAgent"] = (
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
            )
            self.driver = webdriver.PhantomJS(desired_capabilities=dcap)
        self.driver.implicitly_wait(3)
        self.url_extractor = tldextract.TLDExtract(suffix_list_url=False)

    def get_domain(self, url):
        url = url.strip()
        r = self.url_extractor(url)
        result = '.'.join((r.domain, r.suffix))
        if result == 'http.':
            logging.error('%s may not processed correctly!!!')
        return result

    def crawl(self, keyword):
        pass

    def download(self, keyword):
        pass

    def parse(self, htmldoc, keyword):
        #TODO: Monitor correctness/ change
        result = {"result": {}}
        soup = BeautifulSoup(htmldoc)
        hasErr = False
        for resultname, extractor in self.extractors.iteritems():
            try:
                logging.info('extracting:%s' % resultname)
                links = extractor.extract(soup)
                logging.info('extracted:%s for %s' % (len(links), keyword))
                result['result'][resultname] = [self.get_domain(link) for link in links]
            except Exception as e:
                hasErr = True
                logging.error('extractor error [%s]' % resultname)
                logging.error(e.message)

        if hasErr is True \
                and 'error_scrrenshot' in self.options \
                and self.options['error_screenshot'] is True:
            try:
                self.driver.save_screenshot(
                    '%s/%s-error-%s-%s.png' % (
                        self.source,
                        'error', self.options['error_screenshot_path'],
                        keyword,
                        self.timestamp))
            except Exception as e:
                logging.error(e)
        return result


class Extractor(object):
    def extract(self, soup):
        pass

