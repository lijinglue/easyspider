#!/usr/bin/env python
import threading
import sys
import optparse
import logging
import time
import json
import uuid

from tornado.web import RequestHandler, asynchronous, StaticFileHandler
from tornado.web import Application
from tornado import ioloop
from pymongo import MongoClient
import requests
import codecs
import os

from easyspider.spider.baidu import BaiduSpider

from easyspider.spider.qihu import QihuSpider


client = MongoClient('localhost', 27017)
db = client.sylebra

root = logging.getLogger()
root.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

if __name__ == '__main__':

    def create_if_not(cur, fields):
        for k, v in fields.iteritems():
            if k not in cur:
                cur[k] = v

    parser = optparse.OptionParser()
    parser.add_option("-f", "--file", dest="file",
                      help="The file contains keywords",
                      metavar="FILE")
    parser.add_option("-i", "--interval", dest="interval",
                      help="interval of each download action in second, default is 10s",
                      metavar="INTERVAL")
    parser.add_option("-o", "--output", dest="output",
                      help="output file",
                      metavar="OUT")
    parser.add_option("-c", "--config", dest="config",
                      help="config file",
                      metavar="CONF")

    (options, args) = parser.parse_args()

    default_config_path = '../etc/spider.config.json'
    if not options.config and os.path.exists(default_config_path):
        options.config = default_config_path
    if options.config:
        options.config = json.load(open(options.config))

    class CrawlWorker(object):
        def __init__(self, spider, config):
            self.task = []
            self.report_url = config['master_url'] + "/report"
            self.interval = config['keyword_interval']
            self.spider = spider(options=config)
            self.spider_thread = None
            self.resultCache = None
            # TODO: result caching

        def add_task(self, task):
            self.task += task
            if not self.spider_thread or not self.spider_thread.isAlive():
                self.spider_thread = threading.Thread(target=self._crawl)
                self.spider_thread.start()

        def _crawl(self):
            while self.task:
                start = time.time()
                word = self.task.pop(0)
                post_body = json.dumps(self.spider.crawl(word.rstrip('\n')), ensure_ascii=False)
                try:
                    logging.info('requesting report')
                    response = requests.post(self.report_url, post_body.encode('utf-8'))  #TODO better error handling
                    logging.info('report status code: %s' % response.status_code)
                except Exception as e:
                    logging.error(e)
                end = time.time()
                delta = end - start
                logging.info("%s from %s consumed %s" % (word, self.spider.source, delta))
                if delta < self.interval:
                    logging.warning("Defensive sleeping")
                    time.sleep(self.interval - delta)

    class TaskMgr(object):
        def __init__(self, config):
            self.config = config
            self.task_queue = []
            self.is_working = False
            self.report_url = config['master_url'] + "/report"
            self.interval = config['keyword_interval']
            self.spider_reg = None
            self.spiders = None

        def add_task(self, tasks):
            self.task_queue += tasks
            self.work()

        def work(self):
            if not self.spiders:
                self.spider_reg = {
                    'baidu': BaiduSpider,
                    'qihu': QihuSpider
                }

                self.spiders = [CrawlWorker(self.spider_reg[sp], self.config)
                                for sp in options.config['active_spider']]
                logging.info('browser is ready, processing keywords')
            for spider in self.spiders:
                spider.add_task(self.task_queue)

    task_mgr = TaskMgr(options.config)
    reports = []

    class BaseRequestHandler(RequestHandler):
        def set_default_headers(self):
            self.set_header("Access-Control-Allow-Origin", "*")
            self.set_header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
            self.set_header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, *")

        def options(self, *args, **kwargs):
            self.set_status(200)
            self.finish()

    class UploadHandler(BaseRequestHandler):
        def put(self):
            if 'fileUpload' in self.request.files:
                fileinfo = self.request.files['fileUpload'][0]
            else:
                fileinfo = self.request.files['file'][0]
            fname = fileinfo['filename']
            tempath = os.path.join('/tmp', 'keywords-%s.txt' % uuid.uuid4())
            with codecs.open(tempath, 'w', 'utf-8') as tempfile:
                tempfile.write(fileinfo['body'].decode('utf-8'))
            with codecs.open(tempath, 'r', 'utf-8') as tempfile:
                kw = [ln.rstrip() for ln in tempfile]
                task_mgr.add_task(kw)
            self.finish()


    class QueuedPersistence(object):
        def __init__(self, db):
            self.db = db
            self.data_queue = []
            # with GIL, lists are thread safe
            self.persist_thread = None

        def add_task(self, data):
            self.data_queue.append(data)
            if not self.persist_thread or not self.persist_thread.isAlive():
                self.persist_thread = threading.Thread(target=self._persists)
            self.persist_thread.start()

        def persist_queued(self):
            self._persists()


        def _merge_dict_of_lists(self, base, addition):
            for key, value in addition.iteritems():
                if not key in base:
                    base[key] = value
                else:
                    base[key] = list(set(base[key]) | set(value))
                return base

        def _persists(self):
            while self.data_queue:
                data = self.data_queue.pop(0)
                source = data['source']
                keyword = data['keyword']
                brandlinks = [] if not 'brandlinks' in data['result'] else data['result']['brandlinks']
                advertisers = []
                for _, advs in data['result'].iteritems():
                    advertisers += advs

                keyword_repo = self.db['keywords']
                advertiser_repo = self.db['advertisers']
                brand_repo = self.db['brands']

                if brandlinks:
                    for brandlink in brandlinks:
                        brand = brand_repo.find_one({'domain': brandlink})
                        if not brand:
                            brand = {'domain': brandlink, 'source': {source: []}}
                        if not source in brand['source']:
                            brand['source'] = {source: [keyword]}
                        if not keyword in brand['source'][source]:
                            brand['source'][source].append(keyword)
                        brand_repo.save(brand)

                keyword_entity = keyword_repo.find_one({"keyword": data['keyword']})
                if not keyword_entity:
                    keyword_entity = {'keyword': keyword, 'raw_result': data['result']}
                if not 'advertisers' in keyword_entity:
                    keyword_entity['advertisers'] = []
                if not 'source' in keyword_entity:
                    keyword_entity['source'] = {source: []}
                if not source in keyword_entity['source']:
                    keyword_entity['source'][source] = []

                keyword_entity['advertisers'] = list(set(advertisers) | set(keyword_entity['advertisers']))
                current_source_adv = keyword_entity['source'][source]
                keyword_entity['source'][source] = list(set(advertisers) | set(current_source_adv))
                keyword_entity['raw_result'] = self._merge_dict_of_lists(keyword_entity['raw_result'], data['result'])

                keyword_repo.save(keyword_entity)

                for adv in advertisers:
                    advertiser = advertiser_repo.find_one({"domain": adv})
                    if not advertiser:
                        advertiser = {'domain': adv, 'keyword': [], 'source': {}}
                    if not keyword in advertiser['keyword']:
                        advertiser['keyword'].append(keyword)
                    if not source in advertiser['source']:
                        advertiser['source'][source] = {}
                    current_source_keyword = advertiser['source'][source]
                    if not keyword in current_source_keyword:
                        advertiser['source'][source][keyword] =[]
                    current_ad_position = [section for section, adv_list in data['result'].iteritems() if advertiser in adv_list]
                    # current ad position of the advertiser
                    advertiser['source'][source][keyword] = list(set(advertiser['source'][source][keyword])|set(current_ad_position))
                    advertiser_repo.save(advertiser)

    queued_persistence = QueuedPersistence(db)

    class ReportHandler(BaseRequestHandler):
        def post(self, *args, **kwargs):  # accept multiple report entity
            reports.append(self.request.body)
            data = json.loads(self.request.body)
            queued_persistence.add_task(data)

        def get(self):
            output = None
            filename = 'report-%s.csv' % uuid.uuid4()
            filepath = '/tmp/%s' % filename
            aggr = self.get_argument('aggr', 'keywords', strip=True)
            if aggr == 'keywords':
                keyword_repo = db['keywords']
                keywords = keyword_repo.find()
                result = [','.join(('keyword', 'advertisers', 'display position'))]
                with codecs.open(filepath, 'w', 'utf-8') as opt:
                    for keyword in keywords:
                        #result.append(','.join((keyword['keyword'], ','.join(keyword['advertisers']))))
                        if 'raw_result' in keyword and keyword['raw_result']:
                            for position, advertisers in keyword['raw_result'].iteritems():
                                for adv in advertisers:
                                    result.append(','.join([keyword['keyword'], adv, position]))
                    opt.write('\n'.join(result))
                    opt.flush()
            elif aggr == 'advertisers':
                advertiser_repo = db['advertisers']
                advertisers = advertiser_repo.find()
                result = [','.join(('advertiser', 'keywords'))]
                with codecs.open(filepath, 'w', 'utf-8') as opt:
                    for adv in advertisers:
                        result.append(','.join((adv['domain'], ','.join(adv['keyword']))))
                    opt.write('\n'.join(result))
                    opt.flush()
            elif aggr == 'brand':
                result = [','.join(('domain', 'baidu', 'qihu'))]
                brand_repo = db['brands']
                brands = brand_repo.find()
                if brands:
                    for brand in brands:
                        line = [brand['domain'],
                                '1' if 'baidu' in brand['source'] else '0',
                                '1' if 'qihu' in brand['source'] else '0']
                        result.append(','.join(line))
                with codecs.open(filepath, 'w', 'utf-8') as opt:
                    opt.write('\n'.join(result))
                    opt.flush()
            elif aggr == 'coverage':
                result = [','.join(('keyword', 'baidu', 'qihu'))]
                keyword_repo = db['keywords']
                keywords = keyword_repo.find()
                for keyword in keywords:
                    line = [keyword['keyword'],
                            str(len(keyword['source']['baidu'])),
                            str(len(keyword['source']['qihu']))]
                    result.append(','.join(line))
                with codecs.open(filepath, 'w', 'utf-8') as opt:
                    opt.write('\n'.join(result))
                    opt.flush()
            else:
                self.set_status(400)
                self.finish()

            self.redirect('/archived/%s' % filename)

    class TaskHandler(BaseRequestHandler):

        def get(self):

            if not task_mgr.spiders:
                num = 0
            else:
                num = sum([len(q.task) for q in task_mgr.spiders])
            self.write({
                'remaining': num
            })

        @asynchronous
        def post(self, *args, **kwargs):
            request_body = json.loads(self.request.body)
            keywords = request_body['keywords']
            self.set_status(200)
            self.finish()
            task_mgr.add_task(keywords)

    application = Application([
        (r"/task", TaskHandler),
        (r"/report", ReportHandler),
        (r"/upload", UploadHandler),
        (r"/archived/(.*)", StaticFileHandler, {"path": "/tmp"})
    ])

    application.listen(13296)
    ioloop.IOLoop.instance().start()
