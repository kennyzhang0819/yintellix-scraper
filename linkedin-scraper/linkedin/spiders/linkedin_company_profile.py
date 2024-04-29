import json
import scrapy
import pandas as pd
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from scrapy import signals
from scrapy.signalmanager import dispatcher
import time
import re
import numpy as np
from collections import Counter
from bs4 import BeautifulSoup

class GetCompanyLinksSpider(scrapy.Spider):
    name = "linkedin_jobs"
    api_url = api_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Sales%2BDevelopment%2BRepresentative&location=United%2BStates&geoId=103644278&trk=public_jobs_jobs-search-bar_search-submit&start='  
    company_links = []
    max_company = 10
    
    def start_requests(self):
        first_job_on_page = 0
        first_url = self.api_url + str(first_job_on_page)
        yield scrapy.Request(url=first_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})


    def parse_job(self, response):
        first_job_on_page = response.meta['first_job_on_page']

        job_item = {}
        jobs = response.css("li")

        num_jobs_returned = len(jobs)
        print("******* Num Jobs Returned *******")
        print(num_jobs_returned)
        print('*****')
        
        for job in jobs:
            
            job_item['job_title'] = job.css("h3::text").get(default='not-found').strip()
            job_item['job_detail_url'] = job.css(".base-card__full-link::attr(href)").get(default='not-found').strip()
            job_item['job_listed'] = job.css('time::text').get(default='not-found').strip()

            job_item['company_name'] = job.css('h4 a::text').get(default='not-found').strip()
            job_item['company_link'] = job.css('h4 a::attr(href)').get(default='not-found')
            job_item['company_location'] = job.css('.job-search-card__location::text').get(default='not-found').strip()
            self.company_links.append({'company_name': job_item['company_name'], 'company_link': job_item['company_link']})
        

        if len(self.company_links) < self.max_company:
            first_job_on_page = int(first_job_on_page) + 25
            next_url = self.api_url + str(first_job_on_page)
            yield scrapy.Request(url=next_url, callback=self.parse_job, meta={'first_job_on_page': first_job_on_page})
        
        df = pd.DataFrame(self.company_links)
        df.drop_duplicates(subset=['company_link'], inplace=True)
        df.to_csv('companies.csv', index=False)

class GetCompanyProfileSpider(scrapy.Spider):
    name = "linkedin_company_profile"
    company_data = []
    size = 0

    def start_requests(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        
        self.company_pages = []
        df = pd.read_csv('companies.csv')
        self.company_pages = df['company_link']
        self.company_pages = list(set(self.company_pages))
        self.size = len(self.company_pages)
        for index, url in enumerate(self.company_pages):
            yield scrapy.Request(url=url, callback=self.parse_response, meta={'index': index})
        
    def parse_response(self, response):
        index = response.meta['index']
        print('****** processing ' + str(index) + ' / ' + str(self.size))

        company_item = {}

        company_item['name'] = response.css('.top-card-layout__entity-info h1::text').get(default='not-found').strip()
        company_item['summary'] = response.css('.top-card-layout__entity-info h4 span::text').get(default='not-found').strip()
        company_item['job_link'] = response.css('.top-card-layout__cta-container a::attr(href)').get(default='not-found').strip()

        try:
            company_item['company_description'] = response.css('.core-section-container__content .whitespace-pre-wrap::text').get(default='not-found').strip()
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
    
    def spider_closed(self, spider):
        df = pd.DataFrame(self.company_data)
        df.to_csv('company_profiles.csv', encoding='utf-8', index=False)

class CompanyJobsMatcherSpider(scrapy.Spider):
    name = "linkedin_company_jobs"
    company_job_links = []
    size = 0
    
    
    def start_requests(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        df = pd.read_csv('company_profiles.csv')
        df = df[["name", "job_link"]]
        # df['job_link'] = df['job_link'].apply(self.transform_url)
        self.size = len(df)
        for index, url in enumerate(df['job_link']):
            yield scrapy.Request(url=url, callback=self.parse_job, meta={'index': index})
        
        
    def parse_job(self, response):
        index = response.meta['index']
        print('****** processing ' + str(index) + ' / ' + str(self.size))
        jobs = response.css(".base-card")
        
        for job in jobs:
            company_name = job.css(".base-search-card__subtitle a::text").get(default='not-found').strip()
            job_link = job.css("a::attr(href)").get(default='not-found').strip()
            job_title = job.css("h3::text").get(default='not-found').strip()
            job_location = job.css(".job-search-card__location::text").get(default='not-found').strip()
            self.company_job_links.append({'company_name': company_name, 
                                           'job_link': job_link, 
                                           'job_title': job_title, 
                                           'job_location': job_location})
            
    def spider_closed(self, spider):
        df = pd.DataFrame(self.company_job_links)
        df.to_csv('company_jobs_relation.csv', encoding='utf-8', index=False)
        
    def transform_url(self, url):
        parts = url.split('/')
        if 'jobs' in parts:
            new_url = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/' + parts[-1] + '&start='
            return new_url
        return url

class GetJobSpecificsSpider(scrapy.Spider):
    name = "linkedin_company_jobs_specifics"
    company_job_data = []
    size = 0

    def start_requests(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        df = pd.read_csv('company_jobs_relation.csv')
        self.size = len(df)
        
        for index, url in enumerate(df['job_link']):
            print("****** requesting " + str(index))
            yield scrapy.Request(url=url, callback=self.parse_response, meta={'url': url})
        
    def parse_response(self, response):
        job_specific_item = {}
        
        card_info = response.css('.top-card-layout__entity-info')
        job_specific_item['name'] = card_info.css('h1::text').get(default='not-found').strip()
        job_specific_item['company'] = card_info.css('h4 span a::text').get(default='not-found').strip()
        job_specific_item['job_url'] = response.meta['url']

        soup = BeautifulSoup(response.text, "html.parser")
        
        salary_div = soup.find('div', class_='compensation__salary')
        job_specific_item['salary'] = salary_div.get_text(strip=True) if salary_div else "not-found"

        description_section = soup.find('section', class_='show-more-less-html')
        job_specific_item['description'] = description_section.get_text(strip=True) if description_section else "not-found"
    
        self.company_job_data.append(job_specific_item)
    
    def spider_closed(self, spider):
        df = pd.DataFrame(self.company_job_data)
        df.to_csv('company_job_specifics.csv', encoding='utf-8', index=False)


def calculateStats():
    specifics = pd.read_csv('company_job_specifics.csv')
    profiles = pd.read_csv('company_profiles.csv')
    relation = pd.read_csv('company_jobs_relation.csv')

    jobs_with_locations = pd.merge(relation, specifics, how='left', left_on='job_link', right_on='job_url')
    full_data = pd.merge(profiles, jobs_with_locations, how='left', left_on='name', right_on='company')
    
    def parse_and_normalize_salary(salary_str):
        # Check if the salary field is NaN
        if pd.isna(salary_str) or salary_str == 'not-found':
            return None
        # Find all monetary amounts in the salary string
        salary_range = re.findall(r'\$\d+,\d+', salary_str)
        salary_range = [int(x.replace(',', '').replace('$', '')) for x in salary_range]
        if "/yr" in salary_str:
            # If salary is annual, use as is
            annual_salary = salary_range
        elif "/hr" in salary_str:
            # If salary is hourly, convert to annual (assuming 2080 hours per year)
            annual_salary = [x * 2080 for x in salary_range]
        else:
            return None
        # Calculate the mean of the salary range
        if len(annual_salary) == 2:
            return np.mean(annual_salary)
        elif len(annual_salary) == 1:
            return annual_salary[0]
        else:
            return None

    full_data['normalized_salary'] = full_data['salary'].apply(parse_and_normalize_salary)

    def most_frequent_word(texts):
        stop_words = set(["the", "of", "this", "and", "in", "to", "a", "for", "your", "you", "or", "we", "with", "be", "our", "on", "is", "are", "not", "all"])
        words = re.findall(r'\b\w+\b', ' '.join(texts).lower())
        words = [word for word in words if word not in stop_words]
        most_common = Counter(words).most_common(1)
        return most_common[0][0] if most_common else None

    grouped = full_data.groupby('name_x').agg({
        'summary': 'first',
        'job_location': lambda x: x.value_counts().to_dict() if pd.notna(x).any() else {},
        'description': lambda x: most_frequent_word(x) if pd.notna(x).any() else None,
        'normalized_salary': ['mean', 'median', 'std', 'max']
    }).reset_index()

    grouped.columns = [
        'Company Name', 'Company Description', 'Location Distribution',
        'Most Used Word in Descriptions', 'Average Salary', 'Median Salary', 
        'Salary Std Dev', 'Highest Paid Role'
    ]

    grouped.to_csv('final_stats.csv', encoding='utf-8', index=False)


    
settings = get_project_settings()
configure_logging(settings)
runner = CrawlerRunner(settings)

@defer.inlineCallbacks
def crawl():
    yield runner.crawl(GetCompanyLinksSpider)
    yield runner.crawl(GetCompanyProfileSpider)
    yield runner.crawl(CompanyJobsMatcherSpider)
    yield runner.crawl(GetJobSpecificsSpider)
    calculateStats()
    reactor.stop()

crawl()
reactor.run()