#!/usr/bin/env python
import sys
import optparse
import logging
import time
import json

import codecs
import os

from easyspider.spider.baidu import BaiduSpider


root = logging.getLogger()
root.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)


if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option("-f", "--file", dest="file",
                      help="The file contains keywords",
                      metavar="FILE")
    parser.add_option("-o", "--output", dest="output",
                      help="output file",
                      metavar="OUT")
    parser.add_option("-c", "--config", dest="config",
                      help="config file",
                      metavar="CONF")

    (options, args) = parser.parse_args()

    default_config_path = '../etc/spider.config.json'
    if not options.file:
        raise parser.error('Please specify keyword file')
    if not options.output:
        options.output = 'output.log'
    if not options.config and os.path.exists(default_config_path):
        options.config = default_config_path
    if options.config:
        options.config = json.load(open(options.config))

    try:
        input_file = codecs.open(options.file, 'r', 'utf-8')
        spider = BaiduSpider(options=options.config)
        interval = options.config['keyword_interval']
        with codecs.open(options.output, 'a', 'utf-8') as output:
            for word in input_file:
                start = time.time()
                output.write(json.dumps(spider.crawl(word.rstrip('\n')), ensure_ascii=False))
                output.write(u'\n')
                output.flush()
                end = time.time()
                delta = end - start
                if delta < interval:
                    time.sleep(interval - delta)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
