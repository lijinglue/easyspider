EasySpider
===

## System Requirements ##
1. Bash 4
2. Python 2.7+ , pip
3. gcc, lxml
4. mongodb
5. Any of Selenium drivers: chrome-driver, phantomjs-driver, firefox-driver ~

## Intro ##

A Crawler tracks down AD links on baidu and qihu. 
Major features: 

* Crawling service
* Crawl task management
* Result gathering and report generation


For the sake of simplicity and ease of demo, code for report server, crawl server,
crawl task server are all coded in net_runner.py.

## Configuration ##

Main configration file is in ./etc/spider.config.json

## Start service ##
1. (If the package is not on PYTHONPATH or system path)use ./activate to set the PYTHONPATH to project root.
2. run the net_runner.py


