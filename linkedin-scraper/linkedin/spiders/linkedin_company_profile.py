import json
import scrapy
import pandas as pd
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
import time

class GetCompanyLinksSpider(scrapy.Spider):
    name = "linkedin_jobs"
    api_url = f'https://www.linkedin.com/jobs/search?keywords=Sales%20Development%20Representative&location=United%20States&geoId=103644278&trk=public_jobs_jobs-search-bar_search-submit&position=1&pageNum={0}' 
    company_links = []
    
    def start_requests(self):
        first_job_on_page = 0
        first_url = self.api_url.format(first_job_on_page)
        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})


    def parse_job(self, response):
        first_job_on_page = response.meta['first_job_on_page']

        job_item = {}
        jobs = response.css(".base-search-card__info")

        num_jobs_returned = len(jobs)
        print("******* Num Jobs Returned *******")
        print(num_jobs_returned)
        print('*****')
        
        for job in jobs:
            
            job_link = job.css("a::attr(href)").get(default='not-found').strip()
            job_title = job.css("h3::text").get(default='not-found').strip()
            self.company_links.append({'company_link': job_link, 'job_title': job_title})
            
        

        # if num_jobs_returned > 0 and first_job_on_page < 3:
        #     first_job_on_page = first_job_on_page + 1
        #     print(first_job_on_page)
        #     next_url = self.api_url.format(first_job_on_page)
        #     print('***************')
        #     print(next_url)
        #     yield scrapy.Request(url=next_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})
        
        with open('companies.json', 'w') as outfile:
            json.dump(self.company_links, outfile, indent=4)

class GetCompanyProfileSpider(scrapy.Spider):
    name = "linkedin_company_profile"
    api_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=python&location=United%2BStates&geoId=103644278&trk=public_jobs_jobs-search-bar_search-submit&start=' 

    #cutstom test
    company_pages = [
        'https://www.linkedin.com/company/michael-page/',
        'https://www.linkedin.com/company/hays/',
        ]

    company_data = []

    def start_requests(self):
        company_index_tracker = 0
        self.readUrlsFromCompaniesFile()
        first_url = self.company_pages[company_index_tracker]

        yield scrapy.Request(url=first_url, callback=self.parse_response, meta={'company_index_tracker': company_index_tracker})
        
    def parse_response(self, response):
        company_index_tracker = response.meta['company_index_tracker']
        print('***************')
        print('****** Scraping page ' + str(company_index_tracker+1) + ' of ' + str(len(self.company_pages)))
        print('***************')

        company_item = {}

        company_item['name'] = response.css('.top-card-layout__entity-info h1::text').get(default='not-found').strip()
        company_item['summary'] = response.css('.top-card-layout__entity-info h4 span::text').get(default='not-found').strip()
        company_item['job_link'] = response.css('.top-card-layout__cta-container a::attr(href)').get(default='not-found').strip()

        try:
            ## all company details 
            company_item['description'] = response.css('.core-section-container__content .whitespace-pre-wrap::text').get(default='not-found').strip()
            company_details = response.css('.core-section-container__content .mb-2')
        
            company_industry_line = company_details[1].css('.text-md::text').getall()
            company_item['industry'] = company_industry_line[1].strip()

            company_size_line = company_details[2].css('.text-md::text').getall()
            company_item['size'] = company_size_line[1].strip()

            company_size_line = company_details[5].css('.text-md::text').getall()
            company_item['founded'] = company_size_line[1].strip()
            
            company_specialities_line = company_details[6].css('.text-md::text').getall()
            company_item['specialities'] = company_specialities_line[1].strip()
        except IndexError:
            print("Some details missing for this company. Skipping those details.")
            
        

        self.company_data.append(company_item)
        company_index_tracker = company_index_tracker + 1

        if company_index_tracker <= (len(self.company_pages)-1):
            next_url = self.company_pages[company_index_tracker]

            yield scrapy.Request(url=next_url, callback=self.parse_response, meta={'company_index_tracker': company_index_tracker})
        else:
            df = pd.DataFrame(self.company_data)
            df['description'] = df['description'].apply(lambda x: x.replace('鈥檚', "").replace('鈥檙', "'r"))
            df.to_csv('company_profiles.csv', encoding='utf-8', index=False)
            
    def readUrlsFromCompaniesFile(self):
        self.company_pages = []
        with open('companies.json') as file:
            companiesFromFile = json.load(file)

            for company in companiesFromFile:
                if company['company_link'] != 'not-found':
                    self.company_pages.append(company['company_link'])
            
        self.company_pages = list(set(self.company_pages))




settings = get_project_settings()
configure_logging(settings)
runner = CrawlerRunner(settings)

@defer.inlineCallbacks
def crawl():
    yield runner.crawl(GetCompanyLinksSpider)
    yield runner.crawl(GetCompanyProfileSpider)
    reactor.stop()

crawl()
reactor.run()