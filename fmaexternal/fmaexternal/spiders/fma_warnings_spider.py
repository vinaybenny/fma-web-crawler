# -*- coding: utf-8 -*-
"""
@created: 09 July 2019
@author: vinay.benny
@description: This spider is designed to crawl the domain mentioned under "ALLOWED_DOMAINS" declared below, to extract FMA-issued
    warnings and other related details. The design of the crawler is based on the HTML structure of the target domains as on
    the creation date. The spider parses the HTML content, and creates a JSON dump of the page with the parsed content and
    other metadata. Currently the JSON file will contain the speech title, the date of speech, the URL to the page, the JSON filename,
    and the content of the speech.
    
    The spider accepts the following command line parameters:
        1. "results_to_crawl": the number of search results to crawl within the result pages-if not supplied, all warnings are scraped.
        
    Invoke the script as follows (from "./fma-web-crawlers/fmaexternal" folder): 
        scrapy crawl fmawarnings -a <results_to_scrawl: positive integer>
"""
import os
import scrapy
import json
from bs4 import BeautifulSoup
import re


# Global constants for the spider
ALLOWED_DOMAINS = ['www.www.fma.govt.nz']
ROOT_URL = 'https://www.fma.govt.nz/news-and-resources/warnings-and-alerts/?start=%s'
DATA_DIRECTORY = "../data/data_scraped/"    # Target directory for JSON files
RESULTS_TO_CRAWL = None                     # Default number of pages to crawl, within the results fetched by date filters 
custom_settings = {
        "DOWNLOAD_DELAY": 5,                # Delay (in seconds) between successive hits on the domain
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2
    }

class FMAWarningsSpider(scrapy.Spider):
    name = "fmawarnings"
    root_url = ROOT_URL
    
    def __init__(self, results_to_crawl=RESULTS_TO_CRAWL, *args, **kwargs):
        super(FMAWarningsSpider, self).__init__(*args, **kwargs)
        self.results_to_crawl = results_to_crawl

    def start_requests(self):
        next_page = "1"
        yield scrapy.Request(url = self.root_url % next_page, callback = self.parse_resultlist)

    def parse_resultlist(self, response):
        if self.results_to_crawl == None:
            self.results_to_crawl = re.search(r'\d+', response.xpath('//div/*[contains(@class, "numResults")]/text()').get()).group()
        for page in range(1, self.results_to_crawl + 1):
                yield scrapy.Request(self.root_url % page, callback = self.parse_results)

    def parse_results(self, response):
        yield scrapy.Request(response.xpath('//*[contains(@class, "result_path_link")]/@href').extract_first())
    
    def parse(self, response):
        # Create folders based on dates of speeches
        filepath = DATA_DIRECTORY
        filename = 'warning' + response.url.replace("/", "")[-40:]
        data = {}
        soup = BeautifulSoup(response.body, 'lxml')        
        
        if not os.path.exists(filepath):
            os.makedirs(filepath)

        with open(filepath + "/" + filename + '.txt', 'w', encoding='utf-8') as datafile:
            content = u""
            # For speech content, we have 2 rules:
            #   1.Check inside <p> tag with appropriate class name, and find all direct child paragraphs
            paragraphs = soup.find('div', class_= "standard_content highlightable").find_all( \
                                  lambda tag: tag and tag.name=="p" \
                                  , recursive=False)    
            
            for item in paragraphs:
                content = content + str(item.text) +  ('\r\n')
            
            data['content'] = content
            json.dump(data, datafile, ensure_ascii=False)
                


 