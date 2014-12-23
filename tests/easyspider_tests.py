# -*- coding: utf-8 -*-
from nose.tools import *
import easyspider
from easyspider.spider.baidu import BaiduSpider

def setup():
    print "SETUP!"

def test_baidu():
    bs = BaiduSpider()
    result = bs.crawl(u'比特币')
    print result

def teardown():
    print "TEAR DOWN!"
